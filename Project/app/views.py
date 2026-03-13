from django.shortcuts import render
from django.shortcuts import render, redirect
from django.contrib.auth.models import User
from django.contrib.auth import logout
from django.contrib import messages
from django.shortcuts import render
from .models import Crane   # change if your model name is different
from django.core.paginator import Paginator
from django.http import HttpResponse, JsonResponse
import csv
from django.contrib.auth import authenticate, login as auth_login
from django.contrib import messages
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from django.views.decorators.cache import never_cache
from django.http import Http404
from django.views.decorators.csrf import csrf_protect, ensure_csrf_cookie
from dateutil.relativedelta import relativedelta
from .models import ChangeHistory, Crane, CraneDueTracking, CranePaymentHistory, Termination
from datetime import datetime
from datetime import date
from datetime import timedelta
from django.utils import timezone
from django.shortcuts import get_object_or_404
from django.db import transaction
from django.db.models import Count, Exists, OuterRef, Q, CharField
from django.db.models.functions import Cast
from django.conf import settings
from django.urls import reverse


def _log_change(request, action, crane=None, details=''):
    if crane is not None:
        crane_display_id = str(crane.id)
        crane_kunde = crane.kunde or ''
    else:
        crane_display_id = ''
        crane_kunde = ''

    user = request.user if getattr(request, 'user', None) and request.user.is_authenticated else None

    ChangeHistory.objects.create(
        crane=crane,
        crane_display_id=crane_display_id,
        crane_kunde=crane_kunde,
        action=action,
        details=details,
        changed_by=user,
    )


def _parse_iso_date(value):
    if not value:
        return None

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    value_str = str(value).strip()
    if not value_str:
        return None

    date_part = value_str.replace("T", " ").split()[0]
    normalized = date_part.replace("/", "-").replace(".", "-")

    for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(normalized, fmt).date()
        except ValueError:
            continue

    return None


def _is_license_expired(expiry_value, today_value=None):
    expiry_date = _parse_iso_date(expiry_value)
    if not expiry_date:
        return False

    if today_value is None:
        today_value = date.today()

    return expiry_date < today_value


def _sync_expired_cranes():
    """Auto-mark active cranes as inactive once license expiry date has passed."""
    today_value = date.today()
    active_cranes = Crane.objects.filter(is_active=True).only('id', 'bezahlt_bis_rg_erstellt')
    expired_ids = [
        crane.id
        for crane in active_cranes
        if _is_license_expired(crane.bezahlt_bis_rg_erstellt, today_value)
    ]

    if expired_ids:
        Crane.objects.filter(id__in=expired_ids).update(is_active=False)


def _attach_expiry_flag(cranes):
    today_value = date.today()
    for crane in cranes:
        crane.is_expired = _is_license_expired(crane.bezahlt_bis_rg_erstellt, today_value)


def _due_years_for_crane(crane):
    due_date = _current_due_date_for_crane(crane)

    if not due_date:
        return []

    return [due_date.year]


def _current_due_date_for_crane(crane):
    start_date = _parse_iso_date(crane.lizenzdatum)
    end_date = _parse_iso_date(crane.bezahlt_bis_rg_erstellt)

    if not start_date or not end_date or start_date >= end_date:
        return None

    tracked_due_str = None
    due_status = getattr(crane, 'due_status', None)
    if due_status:
        tracked_due_str = due_status.next_due_date

    tracked_due = _parse_iso_date(tracked_due_str)
    if not tracked_due:
        return start_date

    if tracked_due < start_date:
        return start_date

    if tracked_due >= end_date:
        return None

    return tracked_due


def _next_due_preview_date(current_due, expiry_date):
    if not current_due:
        return None

    next_due = current_due + relativedelta(years=1)
    if expiry_date and next_due > expiry_date:
        next_due = expiry_date

    return next_due


def _restore_due_status_payment_snapshot(due_status):
    latest_payment = due_status.payment_history.order_by('-recorded_at', '-id').first()

    if latest_payment:
        due_status.last_paid_at = latest_payment.recorded_at
        due_status.actual_paid_date = latest_payment.actual_paid_date
    else:
        due_status.last_paid_at = None
        due_status.actual_paid_date = None


def _mark_due_paid_in_background(crane, actual_paid_date=None):
    current_due = _current_due_date_for_crane(crane)
    expiry_date = _parse_iso_date(crane.bezahlt_bis_rg_erstellt)

    if not current_due or not expiry_date:
        return None, None, None

    if actual_paid_date is None:
        actual_paid_date = timezone.localdate()

    next_due = _next_due_preview_date(current_due, expiry_date)
    if not next_due:
        return None, None, None

    with transaction.atomic():
        due_status, _ = CraneDueTracking.objects.get_or_create(crane=crane)
        payment_record = CranePaymentHistory.objects.create(
            due_tracking=due_status,
            paid_for_due_date=current_due,
            actual_paid_date=actual_paid_date,
        )
        due_status.next_due_date = next_due.strftime('%Y-%m-%d')
        due_status.last_paid_at = payment_record.recorded_at
        due_status.actual_paid_date = payment_record.actual_paid_date
        due_status.save(update_fields=['next_due_date', 'last_paid_at', 'actual_paid_date'])

    return current_due, next_due, expiry_date


def _mark_due_unpaid_in_background(crane):
    start_date = _parse_iso_date(crane.lizenzdatum)
    expiry_date = _parse_iso_date(crane.bezahlt_bis_rg_erstellt)

    if not start_date or not expiry_date or start_date >= expiry_date:
        return None, None, None

    due_status = getattr(crane, 'due_status', None)
    tracked_due = _parse_iso_date(due_status.next_due_date) if due_status else None

    if not tracked_due:
        return None, None, None

    previous_due = tracked_due + relativedelta(years=-1)
    if previous_due < start_date:
        return None, None, None

    with transaction.atomic():
        latest_payment = due_status.payment_history.order_by('-recorded_at', '-id').first()
        if latest_payment:
            latest_payment.delete()

        due_status.next_due_date = previous_due.strftime('%Y-%m-%d')
        _restore_due_status_payment_snapshot(due_status)
        due_status.save(update_fields=['next_due_date', 'last_paid_at', 'actual_paid_date'])

    return previous_due, start_date, expiry_date


def _matches_due_filter(crane, year, month_num, day_num):
    due_date = _current_due_date_for_crane(crane)
    if not due_date:
        return False

    if year and year.isdigit() and len(year) == 4:
        if int(year) != due_date.year:
            return False

    if month_num and due_date.month != month_num:
        return False

    if day_num and due_date.day != day_num:
        return False

    return True


def _paid_due_dates_for_crane(crane):
    """Return historical paid due dates for a crane (one entry per paid year)."""
    start_date = _parse_iso_date(crane.lizenzdatum)
    expiry_date = _parse_iso_date(crane.bezahlt_bis_rg_erstellt)

    if not start_date or not expiry_date or start_date >= expiry_date:
        return []

    current_due = _current_due_date_for_crane(crane)
    if current_due:
        last_paid_due = current_due + relativedelta(years=-1)
    else:
        # Fully paid contracts have dues paid up to the year before expiry boundary.
        last_paid_due = expiry_date + relativedelta(years=-1)

    if last_paid_due < start_date:
        return []

    paid_dates = []
    cursor = start_date
    while cursor <= last_paid_due:
        paid_dates.append(cursor)
        cursor = cursor + relativedelta(years=1)

    return paid_dates


def _paid_years_for_crane(crane):
    return [due_date.year for due_date in _paid_due_dates_for_crane(crane)]


def _matches_paid_filter(crane, year, month_num, day_num):
    paid_dates = _paid_due_dates_for_crane(crane)
    if not paid_dates:
        return False

    for paid_date in paid_dates:
        if year and year.isdigit() and len(year) == 4 and int(year) != paid_date.year:
            continue

        if month_num and paid_date.month != month_num:
            continue

        if day_num and paid_date.day != day_num:
            continue

        return True

    return False


def _sort_value(crane, sort_by):
    value = getattr(crane, sort_by, None)

    if value is None:
        return ''

    if isinstance(value, (int, float, bool)):
        return value

    return str(value).lower()


def _with_termination_flag(queryset):
    terminated_subquery = Termination.objects.filter(crane_id=OuterRef('pk'))
    return queryset.annotate(is_terminated=Exists(terminated_subquery))


def _get_due_filtered_queryset(request):
    year = request.GET.get('year', '').strip()
    month = request.GET.get('month', '').strip()
    day = request.GET.get('day', '').strip()

    month_num = None
    day_num = None

    if month.isdigit():
        month_num = int(month)
        if not 1 <= month_num <= 12:
            month_num = None

    if day.isdigit():
        day_num = int(day)
        if not 1 <= day_num <= 31:
            day_num = None

    queryset = list(Crane.objects.filter(is_active=True).select_related('due_status'))
    queryset = [
        crane for crane in queryset
        if _matches_due_filter(crane, year, month_num, day_num)
    ]

    today_value = date.today()

    for crane in queryset:
        current_due = _current_due_date_for_crane(crane)
        expiry_date = _parse_iso_date(crane.bezahlt_bis_rg_erstellt)
        next_due_preview = _next_due_preview_date(current_due, expiry_date)

        crane.next_due_preview = next_due_preview
        crane.is_due_overdue = bool(current_due and current_due < today_value and (not expiry_date or current_due < expiry_date))

    sort_by = request.GET.get('sort', 'id')
    order = request.GET.get('order', 'asc')

    allowed_sorts = [
        'id', 'kran_typ', 'fabrik_nr', 'kunde', 'lg', 'kundenummer',
        'version', 'serien_nr', 'tel_nr', 'ip', 'rueckmeldung', 'it_nr',
        'kundenkran', 'lizenz_ja', 'lizenzdatum', 'bezahlt_bis_rg_erstellt',
        'servicemeldung', 'amount', 'is_active'
    ]
    if sort_by not in allowed_sorts:
        sort_by = 'id'

    queryset.sort(key=lambda crane: crane.id)
    queryset.sort(
        key=lambda crane: _sort_value(crane, sort_by),
        reverse=(order == 'desc')
    )

    return queryset, year, month, day, sort_by, order


def _get_paid_filtered_queryset(request):
    year = request.GET.get('year', '').strip()
    month = request.GET.get('month', '').strip()
    day = request.GET.get('day', '').strip()

    month_num = None
    day_num = None

    if month.isdigit():
        month_num = int(month)
        if not 1 <= month_num <= 12:
            month_num = None

    if day.isdigit():
        day_num = int(day)
        if not 1 <= day_num <= 31:
            day_num = None

    queryset = list(Crane.objects.filter(is_active=True).select_related('due_status'))
    queryset = [
        crane for crane in queryset
        if _matches_paid_filter(crane, year, month_num, day_num)
    ]

    sort_by = request.GET.get('sort', 'id')
    order = request.GET.get('order', 'asc')

    allowed_sorts = [
        'id', 'kran_typ', 'fabrik_nr', 'kunde', 'lg', 'kundenummer',
        'version', 'serien_nr', 'tel_nr', 'ip', 'rueckmeldung', 'it_nr',
        'kundenkran', 'lizenz_ja', 'lizenzdatum', 'bezahlt_bis_rg_erstellt',
        'servicemeldung', 'amount', 'is_active'
    ]
    if sort_by not in allowed_sorts:
        sort_by = 'id'

    queryset.sort(key=lambda crane: crane.id)
    queryset.sort(
        key=lambda crane: _sort_value(crane, sort_by),
        reverse=(order == 'desc')
    )

    return queryset, year, month, day, sort_by, order


def _build_due_filter_context(page_obj, year, month, day, sort_by, order):
    active_cranes = Crane.objects.filter(is_active=True).select_related('due_status')
    years = sorted(
        {
            str(due_year)
            for crane in active_cranes
            for due_year in _due_years_for_crane(crane)
        },
        reverse=True
    )

    return {
        'page_obj': page_obj,
        'years': years,
        'months': list(range(1, 13)),
        'days': list(range(1, 32)),
        'selected_year': year,
        'selected_month': month,
        'selected_day': day,
        'sort_by': sort_by,
        'order': order,
    }


def _build_paid_filter_context(page_obj, year, month, day, sort_by, order):
    active_cranes = Crane.objects.filter(is_active=True).select_related('due_status')
    years = sorted(
        {
            str(paid_year)
            for crane in active_cranes
            for paid_year in _paid_years_for_crane(crane)
        },
        reverse=True
    )

    return {
        'page_obj': page_obj,
        'years': years,
        'months': list(range(1, 13)),
        'days': list(range(1, 32)),
        'selected_year': year,
        'selected_month': month,
        'selected_day': day,
        'sort_by': sort_by,
        'order': order,
    }

@csrf_protect
@ensure_csrf_cookie
@never_cache
def login(request):
    if request.user.is_authenticated:
        return redirect('index')

    logged_out = request.GET.get('logged_out') == '1'

    if request.method == "POST":
        username = request.POST.get("username")
        password = request.POST.get("password")

        user = authenticate(request, username=username, password=password)

        if user is not None:
            auth_login(request, user)
            return redirect('index')

        else:
            messages.error(request, "Invalid username or password")

    return render(request, "login.html", {
        'logged_out': logged_out,
    })


@login_required(login_url='login')
@never_cache
def index(request):
    _sync_expired_cranes()

    today_value = date.today()
    soon_limit = today_value + timedelta(days=30)

    range_from_raw = request.GET.get('from_date', '').strip()
    range_to_raw = request.GET.get('to_date', '').strip()
    range_from = _parse_iso_date(range_from_raw)
    range_to = _parse_iso_date(range_to_raw)

    if range_from and range_to and range_from > range_to:
        range_from, range_to = range_to, range_from

    def _date_in_range(target_date):
        if not target_date:
            return False
        if range_from and target_date < range_from:
            return False
        if range_to and target_date > range_to:
            return False
        return True

    total_cranes = Crane.objects.count()
    active_count = Crane.objects.filter(is_active=True).count()
    inactive_count = Crane.objects.filter(is_active=False).count()
    terminated_ids = set(Termination.objects.values_list('crane_id', flat=True).distinct())
    terminated_count = len(terminated_ids)
    inactive_non_terminated_count = Crane.objects.filter(is_active=False).exclude(id__in=terminated_ids).count()

    overdue_rows = []
    expiring_rows = []
    overdue_buckets = {'1-7 days': 0, '8-30 days': 0, '31+ days': 0}
    expiry_buckets = {'0-7 days': 0, '8-30 days': 0, '31+ days': 0}

    active_cranes = Crane.objects.filter(is_active=True).select_related('due_status').order_by('id')
    for crane in active_cranes:
        current_due = _current_due_date_for_crane(crane)
        expiry_date = _parse_iso_date(crane.bezahlt_bis_rg_erstellt)

        if current_due and current_due < today_value and (not expiry_date or current_due < expiry_date):
            if _date_in_range(current_due) or (not range_from and not range_to):
                days_overdue = (today_value - current_due).days
                overdue_rows.append({
                    'id': crane.id,
                    'kunde': crane.kunde,
                    'serien_nr': crane.serien_nr,
                    'current_due': current_due,
                    'days_overdue': days_overdue,
                })

                if days_overdue <= 7:
                    overdue_buckets['1-7 days'] += 1
                elif days_overdue <= 30:
                    overdue_buckets['8-30 days'] += 1
                else:
                    overdue_buckets['31+ days'] += 1

        if expiry_date and expiry_date >= today_value and (_date_in_range(expiry_date) or (not range_from and not range_to)):
            days_left = (expiry_date - today_value).days

            if days_left <= 7:
                expiry_buckets['0-7 days'] += 1
            elif days_left <= 30:
                expiry_buckets['8-30 days'] += 1
            else:
                expiry_buckets['31+ days'] += 1

            if days_left <= 30:
                expiring_rows.append({
                    'id': crane.id,
                    'kunde': crane.kunde,
                    'serien_nr': crane.serien_nr,
                    'expiry_date': expiry_date,
                    'days_left': days_left,
                })

    recent_changes_queryset = ChangeHistory.objects.select_related('changed_by').all()
    if range_from:
        recent_changes_queryset = recent_changes_queryset.filter(changed_at__date__gte=range_from)
    if range_to:
        recent_changes_queryset = recent_changes_queryset.filter(changed_at__date__lte=range_to)

    recent_changes = recent_changes_queryset[:12]

    activity_start = range_from or (today_value - timedelta(days=29))
    activity_end = range_to or today_value
    if activity_start > activity_end:
        activity_start, activity_end = activity_end, activity_start

    activity_dates = []
    activity_map = {}
    cursor = activity_start
    while cursor <= activity_end:
        activity_dates.append(cursor)
        activity_map[cursor] = 0
        cursor += timedelta(days=1)

    change_activity_queryset = ChangeHistory.objects.filter(
        changed_at__date__gte=activity_start,
        changed_at__date__lte=activity_end,
    ).only('changed_at')
    for changed_at in change_activity_queryset.values_list('changed_at', flat=True):
        changed_date = timezone.localtime(changed_at).date() if timezone.is_aware(changed_at) else changed_at.date()
        if changed_date in activity_map:
            activity_map[changed_date] += 1

    customer_counts = list(
        Crane.objects.exclude(kunde__isnull=True)
        .exclude(kunde__exact='')
        .values('kunde')
        .annotate(total=Count('id'))
        .order_by('-total', 'kunde')[:10]
    )

    total_denominator = max(total_cranes, 1)
    active_denominator = max(active_count, 1)

    total_pct = 100 if total_cranes else 0
    active_pct = round((active_count / total_denominator) * 100)
    inactive_pct = round((inactive_count / total_denominator) * 100)
    terminated_pct = round((terminated_count / total_denominator) * 100)
    overdue_pct = round((len(overdue_rows) / active_denominator) * 100)
    expiring_pct = round((len(expiring_rows) / active_denominator) * 100)

    status_chart = {
        'labels': ['Active', 'Inactive', 'Terminated'],
        'values': [active_count, inactive_non_terminated_count, terminated_count],
    }
    overdue_aging_chart = {
        'labels': list(overdue_buckets.keys()),
        'values': list(overdue_buckets.values()),
    }
    expiry_window_chart = {
        'labels': list(expiry_buckets.keys()),
        'values': list(expiry_buckets.values()),
    }
    change_activity_chart = {
        'labels': [entry.strftime('%m-%d') for entry in activity_dates],
        'values': [activity_map[entry] for entry in activity_dates],
    }
    customer_chart = {
        'labels': [entry['kunde'] for entry in customer_counts],
        'values': [entry['total'] for entry in customer_counts],
    }

    return render(
        request,
        "index.html",
        {
            "group": "All Users",
            "total_cranes": total_cranes,
            "active_count": active_count,
            "inactive_count": inactive_count,
            "terminated_count": terminated_count,
            "overdue_count": len(overdue_rows),
            "expiring_soon_count": len(expiring_rows),
            "overdue_rows": overdue_rows[:8],
            "expiring_rows": expiring_rows[:8],
            "recent_changes": recent_changes,
            "from_date": range_from.isoformat() if range_from else '',
            "to_date": range_to.isoformat() if range_to else '',
            "total_pct": total_pct,
            "active_pct": active_pct,
            "inactive_pct": inactive_pct,
            "terminated_pct": terminated_pct,
            "overdue_pct": overdue_pct,
            "expiring_pct": expiring_pct,
            "status_chart": status_chart,
            "overdue_aging_chart": overdue_aging_chart,
            "expiry_window_chart": expiry_window_chart,
            "change_activity_chart": change_activity_chart,
            "customer_chart": customer_chart,
        },
    )

@login_required(login_url='login')
@never_cache
def data(request):
    _sync_expired_cranes()
    queryset = _with_termination_flag(Crane.objects.all()).order_by('id')

    page_size_raw = request.GET.get('page_size', '10').strip()
    try:
        page_size = int(page_size_raw)
    except (TypeError, ValueError):
        page_size = 10

    if page_size not in (10, 50, 100):
        page_size = 10

    # 📊 EXPORT CSV
    if request.GET.get('export') == 'true':
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="crane_data.csv"'

        writer = csv.writer(response)

        writer.writerow([
            'ID', 'Kran Typ', 'Fabrik Nr', 'Kunde', 'LG', 'Kundenummer',
            'Version', 'Serien Nr', 'Tel Nr', 'IP', 'Rueckmeldung',
            'IT Nr', 'Kundenkran', 'Lizenz Ja', 'Lizenzdatum',
            'Bezahlt bis Rg.erstellt', 'Servicemeldung', 'Amount', 'Status'
        ])

        for crane in queryset:
            if crane.is_terminated:
                status_value = 'Terminated'
            else:
                status_value = 'Active' if crane.is_active else 'Inactive'

            writer.writerow([
                crane.id, crane.kran_typ, crane.fabrik_nr, crane.kunde,
                crane.lg, crane.kundenummer, crane.version,
                crane.serien_nr, crane.tel_nr, crane.ip,
                crane.rueckmeldung, crane.it_nr,
                crane.kundenkran, crane.lizenz_ja,
                crane.lizenzdatum, crane.bezahlt_bis_rg_erstellt,
                crane.servicemeldung, crane.amount, status_value
            ])

        return response

    # 📄 Pagination AFTER export block
    paginator = Paginator(queryset, page_size)
    page_obj = paginator.get_page(request.GET.get('page'))
    _attach_expiry_flag(page_obj.object_list)

    return render(request, 'data_retrival.html', {
        'page_obj': page_obj,
        'page_size': page_size,
        'page_size_options': (10, 50, 100),
    })

   
@never_cache
def logout_view(request):
    logout(request)
    response = redirect(f"{reverse('login')}?logged_out=1")
    response.delete_cookie(settings.SESSION_COOKIE_NAME)
    # Prevent bfcache (back-forward cache) on logout
    response['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0, private'
    response['Pragma'] = 'no-cache'
    response['Expires'] = '0'
    return response

def custom_404(request, exception):
    return render(request, '404.html', status=404)

@login_required
def clear_entry(request, pk):
    kran = get_object_or_404(Crane, pk=pk)

    if request.method == "POST" and kran.lizenzdatum:
        current_due, next_due, _ = _mark_due_paid_in_background(kran)
        if next_due:
            _log_change(
                request,
                'mark_paid',
                crane=kran,
                details=f'Paid from dashboard clear action. Due moved from {current_due} to {next_due}.',
            )

    return redirect('analyst_dashboard')

@login_required
def toggle_status(request, pk):
    """Toggle crane active/inactive status via AJAX"""
    if request.method == "POST":
        kran = get_object_or_404(Crane, pk=pk)
        is_terminated = Termination.objects.filter(crane=kran).exists()

        # Terminated clients must remain inactive permanently.
        if not kran.is_active and is_terminated:
            return JsonResponse(
                {
                    'success': False,
                    'error': 'This client is terminated and cannot be activated.',
                    'is_active': False,
                    'is_terminated': True,
                },
                status=403,
            )

        # Expired licenses cannot be re-activated until expiry is extended.
        if not kran.is_active and _is_license_expired(kran.bezahlt_bis_rg_erstellt):
            return JsonResponse(
                {
                    'success': False,
                    'error': 'License is expired. Update the expiry date to activate.',
                    'is_active': False,
                    'is_terminated': is_terminated,
                    'is_expired': True,
                },
                status=403,
            )

        kran.is_active = not kran.is_active
        kran.save(update_fields=['is_active'])

        _log_change(
            request,
            'status_toggle',
            crane=kran,
            details=f'Status changed to {"Active" if kran.is_active else "Inactive"}.',
        )

        return JsonResponse(
            {
                'success': True,
                'is_active': kran.is_active,
                'is_terminated': is_terminated,
                'is_expired': _is_license_expired(kran.bezahlt_bis_rg_erstellt),
            }
        )
    
    return JsonResponse({'success': False, 'error': 'Invalid request'}, status=400)

@login_required(login_url='login')
@never_cache
def search_rg(request):
    """Search cranes by computed due years and mark selected due as paid."""
    _sync_expired_cranes()
    if request.method == 'POST':
        action = request.POST.get('action', '').strip()
        crane_id = request.POST.get('crane_id', '').strip()
        redirect_url = request.POST.get('next') or '/search_rg/'

        if action == 'mark_paid':
            if not crane_id.isdigit():
                messages.error(request, 'Invalid row selected.')
                return redirect(redirect_url)

            crane = get_object_or_404(
                Crane.objects.select_related('due_status'),
                pk=int(crane_id),
                is_active=True
            )
            current_due = _current_due_date_for_crane(crane)
            expiry_date = _parse_iso_date(crane.bezahlt_bis_rg_erstellt)

            if not current_due or not expiry_date:
                messages.error(
                    request,
                    f'ID {crane.id}: invalid Lizenzdatum or Bezahlt bis date.'
                )
                return redirect(redirect_url)

            paid_date_str = request.POST.get('actual_paid_date', '').strip()
            if not paid_date_str:
                messages.error(request, 'Please provide the actual payment date.')
                return redirect(redirect_url)

            try:
                actual_paid_date = datetime.strptime(paid_date_str, '%Y-%m-%d').date()
            except ValueError:
                messages.error(request, 'Invalid payment date format. Use YYYY-MM-DD.')
                return redirect(redirect_url)

            expected_next_due = _next_due_preview_date(current_due, expiry_date)
            if not expected_next_due:
                return redirect(redirect_url)

            if actual_paid_date.year != expected_next_due.year:
                messages.error(
                    request,
                    f'ID {crane.id}: payment date year must be {expected_next_due.year} (next due year only).'
                )
                return redirect(redirect_url)

            _, next_due, _ = _mark_due_paid_in_background(crane, actual_paid_date=actual_paid_date)
            if not next_due:
                return redirect(redirect_url)

            _log_change(
                request,
                'mark_paid',
                crane=crane,
                details=f'Payment marked with actual payment date {actual_paid_date}. Next due moved to {next_due}.',
            )
            return redirect(redirect_url)

    queryset, year, month, day, sort_by, order = _get_due_filtered_queryset(request)

    if request.GET.get('export') == 'true':
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="crane_rg_due_search.csv"'
        
        writer = csv.writer(response)
        writer.writerow(['ID', 'Kran Typ', 'Fabrik Nr', 'Kunde', 'LG', 'Kundenummer', 
                        'Version', 'Serien Nr', 'Tel Nr', 'IP', 'Rueckmeldung', 'IT Nr',
                        'Kundenkran', 'Lizenz Ja', 'Lizenzdatum', 'Bezahlt bis Rg.erstellt',
                        'Servicemeldung', 'Amount', 'Status'])
        
        for crane in queryset:
            writer.writerow([
                crane.id, crane.kran_typ, crane.fabrik_nr, crane.kunde,
                crane.lg, crane.kundenummer, crane.version, crane.serien_nr,
                crane.tel_nr, crane.ip, crane.rueckmeldung,
                crane.it_nr, crane.kundenkran, crane.lizenz_ja,
                crane.lizenzdatum, crane.bezahlt_bis_rg_erstellt, crane.servicemeldung,
                crane.amount,
                'Active' if crane.is_active else 'Inactive'
            ])
        
        return response

    paginator = Paginator(queryset, 10)
    page_obj = paginator.get_page(request.GET.get('page'))

    context = _build_due_filter_context(page_obj, year, month, day, sort_by, order)
    return render(request, 'search_retrival.html', context)


@login_required(login_url='login')
@never_cache
def search_paid(request):
    """Search cranes by paid history years and mark selected entry as unpaid."""
    _sync_expired_cranes()
    if request.method == 'POST':
        action = request.POST.get('action', '').strip()
        crane_id = request.POST.get('crane_id', '').strip()
        redirect_url = request.POST.get('next') or '/search_paid/'

        if action == 'not_paid':
            if not crane_id.isdigit():
                messages.error(request, 'Invalid row selected.')
                return redirect(redirect_url)

            crane = get_object_or_404(
                Crane.objects.select_related('due_status'),
                pk=int(crane_id),
                is_active=True
            )

            previous_due, _, _ = _mark_due_unpaid_in_background(crane)
            if not previous_due:
                return redirect(redirect_url)

            _log_change(
                request,
                'mark_unpaid',
                crane=crane,
                details=f'Payment reverted. Due moved back to {previous_due}.',
            )
            return redirect(redirect_url)

    queryset, year, month, day, sort_by, order = _get_paid_filtered_queryset(request)

    if request.GET.get('export') == 'true':
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="crane_rg_paid_search.csv"'

        writer = csv.writer(response)
        writer.writerow(['ID', 'Kran Typ', 'Fabrik Nr', 'Kunde', 'LG', 'Kundenummer',
                        'Version', 'Serien Nr', 'Tel Nr', 'IP', 'Rueckmeldung', 'IT Nr',
                        'Kundenkran', 'Lizenz Ja', 'Lizenzdatum', 'Bezahlt bis Rg.erstellt',
                        'Servicemeldung', 'Amount', 'Status', 'Payment Date (Actual)', 'Recorded At (System)'])

        for crane in queryset:
            due = getattr(crane, 'due_status', None)
            actual_paid = str(due.actual_paid_date) if due and due.actual_paid_date else ''
            recorded_at = due.last_paid_at.strftime('%Y-%m-%d %H:%M:%S') if due and due.last_paid_at else ''
            writer.writerow([
                crane.id, crane.kran_typ, crane.fabrik_nr, crane.kunde,
                crane.lg, crane.kundenummer, crane.version, crane.serien_nr,
                crane.tel_nr, crane.ip, crane.rueckmeldung,
                crane.it_nr, crane.kundenkran, crane.lizenz_ja,
                crane.lizenzdatum, crane.bezahlt_bis_rg_erstellt, crane.servicemeldung,
                crane.amount,
                'Active' if crane.is_active else 'Inactive',
                actual_paid,
                recorded_at,
            ])

        return response

    paginator = Paginator(queryset, 10)
    page_obj = paginator.get_page(request.GET.get('page'))

    context = _build_paid_filter_context(page_obj, year, month, day, sort_by, order)
    return render(request, 'search_paid_retrival.html', context)


@login_required(login_url='login')
@never_cache
def update_rg(request):
    """Update expiry date or mark current due year paid for selected crane rows."""
    _sync_expired_cranes()
    query = request.GET.get('q', '').strip()
    queryset = _with_termination_flag(Crane.objects.all()).order_by('id')

    if query:
        query_lower = query.lower()
        queryset = queryset.annotate(
            search_id=Cast('id', output_field=CharField()),
            search_servicemeldung=Cast('servicemeldung', output_field=CharField()),
            search_amount=Cast('amount', output_field=CharField()),
        ).filter(
            Q(search_id__icontains=query)
            | Q(kran_typ__icontains=query)
            | Q(fabrik_nr__icontains=query)
            | Q(kunde__icontains=query)
            | Q(lg__icontains=query)
            | Q(kundenummer__icontains=query)
            | Q(version__icontains=query)
            | Q(serien_nr__icontains=query)
            | Q(tel_nr__icontains=query)
            | Q(ip__icontains=query)
            | Q(rueckmeldung__icontains=query)
            | Q(it_nr__icontains=query)
            | Q(kundenkran__icontains=query)
            | Q(lizenz_ja__icontains=query)
            | Q(lizenzdatum__icontains=query)
            | Q(bezahlt_bis_rg_erstellt__icontains=query)
            | Q(search_servicemeldung__icontains=query)
            | Q(search_amount__icontains=query)
        )

        if query_lower in ('active', 'inactive'):
            queryset = queryset.filter(is_active=(query_lower == 'active'))

        if query_lower == 'terminated':
            queryset = queryset.filter(is_terminated=True)

    if request.method == 'POST':
        action = request.POST.get('action', 'update_expiry').strip()
        crane_id = request.POST.get('crane_id', '').strip()
        new_date = request.POST.get('bezahlt_bis_rg_erstellt', '').strip()
        redirect_url = request.POST.get('next') or '/update_rg/'

        if not crane_id.isdigit():
            messages.error(request, 'Invalid row selected.')
            return redirect(redirect_url)

        crane = get_object_or_404(Crane, pk=int(crane_id))

        if action == 'update_expiry' and Termination.objects.filter(crane=crane).exists():
            messages.error(request, f'ID {crane.id}: terminated rows cannot update expiry date.')
            return redirect(redirect_url)

        if action == 'mark_paid':
            current_due = _current_due_date_for_crane(crane)
            expiry_date = _parse_iso_date(crane.bezahlt_bis_rg_erstellt)

            if not current_due or not expiry_date:
                messages.error(
                    request,
                    f'ID {crane.id}: invalid Lizenzdatum or Bezahlt bis date.'
                )
                return redirect(redirect_url)

            if not current_due or current_due >= expiry_date:
                return redirect(redirect_url)

            paid_date_str = request.POST.get('actual_paid_date', '').strip()
            if not paid_date_str:
                messages.error(request, 'Please provide the actual payment date.')
                return redirect(redirect_url)

            try:
                actual_paid_date = datetime.strptime(paid_date_str, '%Y-%m-%d').date()
            except ValueError:
                messages.error(request, 'Invalid payment date format. Use YYYY-MM-DD.')
                return redirect(redirect_url)

            expected_next_due = _next_due_preview_date(current_due, expiry_date)
            if not expected_next_due:
                return redirect(redirect_url)

            if actual_paid_date.year != expected_next_due.year:
                messages.error(
                    request,
                    f'ID {crane.id}: payment date year must be {expected_next_due.year} (next due year only).'
                )
                return redirect(redirect_url)

            _, next_due, _ = _mark_due_paid_in_background(crane, actual_paid_date=actual_paid_date)
            if not next_due:
                return redirect(redirect_url)

            _log_change(
                request,
                'mark_paid',
                crane=crane,
                details=f'Payment marked from update page with actual payment date {actual_paid_date}. Next due moved to {next_due}.',
            )
            return redirect(redirect_url)

        if not new_date:
            messages.error(request, 'Please provide Bezahlt bis Rg.erstellt date.')
            return redirect(redirect_url)

        try:
            parsed_date = datetime.strptime(new_date, '%Y-%m-%d')
        except ValueError:
            messages.error(request, 'Invalid date format. Use YYYY-MM-DD.')
            return redirect(redirect_url)

        crane.bezahlt_bis_rg_erstellt = parsed_date.strftime('%Y-%m-%d')
        crane.save(update_fields=['bezahlt_bis_rg_erstellt'])

        _log_change(
            request,
            'update_expiry',
            crane=crane,
            details=f'Expiry date updated to {crane.bezahlt_bis_rg_erstellt}.',
        )
        return redirect(redirect_url)

    paginator = Paginator(queryset, 10)
    page_obj = paginator.get_page(request.GET.get('page'))
    _attach_expiry_flag(page_obj.object_list)

    return render(request, 'update.html', {
        'page_obj': page_obj,
        'search_query': query
    })


@login_required(login_url='login')
@never_cache
def create_rg(request):
    """Create new crane entry from create page."""
    if request.method == 'POST':
        form_data = {
            'kran_typ': request.POST.get('kran_typ', '').strip(),
            'fabrik_nr': request.POST.get('fabrik_nr', '').strip(),
            'kunde': request.POST.get('kunde', '').strip(),
            'lg': request.POST.get('lg', '').strip(),
            'kundenummer': request.POST.get('kundenummer', '').strip(),
            'version': request.POST.get('version', '').strip(),
            'serien_nr': request.POST.get('serien_nr', '').strip(),
            'tel_nr': request.POST.get('tel_nr', '').strip(),
            'ip': request.POST.get('ip', '').strip(),
            'rueckmeldung': request.POST.get('rueckmeldung', '').strip(),
            'it_nr': request.POST.get('it_nr', '').strip(),
            'kundenkran': request.POST.get('kundenkran', '').strip(),
            'lizenz_ja': request.POST.get('lizenz_ja', '').strip(),
            'lizenzdatum': request.POST.get('lizenzdatum', '').strip(),
            'bezahlt_bis_rg_erstellt': request.POST.get('bezahlt_bis_rg_erstellt', '').strip(),
            'servicemeldung': request.POST.get('servicemeldung', '').strip(),
            'amount': request.POST.get('amount', '').strip(),
        }

        required_fields = [
            'kran_typ', 'fabrik_nr', 'kunde', 'lg', 'version', 'serien_nr',
            'tel_nr', 'ip', 'rueckmeldung', 'it_nr', 'kundenkran',
            'lizenz_ja', 'lizenzdatum', 'bezahlt_bis_rg_erstellt', 'servicemeldung'
        ]

        missing_fields = [field for field in required_fields if not form_data[field]]
        if missing_fields:
            messages.error(request, 'Please fill all required fields.')
            return render(request, 'create.html', {'form_data': form_data})

        if form_data['kundenkran'] not in ['Kundenkran', 'Mietkran']:
            messages.error(request, 'Invalid Kundenkran value.')
            return render(request, 'create.html', {'form_data': form_data})

        if form_data['lizenz_ja'] not in ['JA', 'NA']:
            messages.error(request, 'Invalid Lizenz Ja value.')
            return render(request, 'create.html', {'form_data': form_data})

        if form_data['lg'] not in ['DE', 'LE']:
            messages.error(request, 'Invalid LG value.')
            return render(request, 'create.html', {'form_data': form_data})

        try:
            lizenzdatum = datetime.strptime(form_data['lizenzdatum'], '%Y-%m-%d').strftime('%Y-%m-%d')
            bezahlt_bis_rg_erstellt = datetime.strptime(
                form_data['bezahlt_bis_rg_erstellt'],
                '%Y-%m-%d'
            ).strftime('%Y-%m-%d')
        except ValueError:
            messages.error(request, 'Dates must be in YYYY-MM-DD format.')
            return render(request, 'create.html', {'form_data': form_data})

        try:
            servicemeldung = int(form_data['servicemeldung'])
        except ValueError:
            messages.error(request, 'Service must be a valid number.')
            return render(request, 'create.html', {'form_data': form_data})

        amount = None
        if form_data['amount']:
            try:
                amount = int(form_data['amount'])
            except ValueError:
                messages.error(request, 'Amount must be a valid number.')
                return render(request, 'create.html', {'form_data': form_data})

        created_crane = Crane.objects.create(
            kran_typ=form_data['kran_typ'],
            fabrik_nr=form_data['fabrik_nr'],
            kunde=form_data['kunde'],
            lg=form_data['lg'],
            kundenummer=form_data['kundenummer'] or None,
            version=form_data['version'],
            serien_nr=form_data['serien_nr'],
            tel_nr=form_data['tel_nr'],
            ip=form_data['ip'],
            rueckmeldung=form_data['rueckmeldung'],
            it_nr=form_data['it_nr'],
            kundenkran=form_data['kundenkran'],
            lizenz_ja=form_data['lizenz_ja'],
            lizenzdatum=lizenzdatum,
            bezahlt_bis_rg_erstellt=bezahlt_bis_rg_erstellt,
            servicemeldung=servicemeldung,
            amount=amount,
            is_active=True,
        )

        _log_change(
            request,
            'create_crane',
            crane=created_crane,
            details='New crane entry created.',
        )
        return redirect('create_rg')

    return render(request, 'create.html', {'form_data': {}})


@login_required(login_url='login')
@never_cache
def terminate_crane(request, pk):
    """Terminate a crane lease early: mark crane inactive and record in Termination table."""
    if request.method != 'POST':
        return redirect('update_rg')

    crane = get_object_or_404(Crane, pk=pk)

    if not crane.is_active:
        messages.error(request, f'ID {crane.id}: already inactive — termination aborted.')
        return redirect('update_rg')

    reason = request.POST.get('reason', '').strip()

    crane.is_active = False
    crane.save(update_fields=['is_active'])

    Termination.objects.create(
        crane=crane,
        terminated_by=request.user,
        termination_reason=reason,
        original_expiry_date=crane.bezahlt_bis_rg_erstellt or '',
        original_lizenzdatum=crane.lizenzdatum or '',
    )

    _log_change(
        request,
        'terminate_crane',
        crane=crane,
        details=f'Lease terminated early. Reason: {reason or "(none)"}.',
    )
    return redirect('update_rg')


@login_required(login_url='login')
@never_cache
def delete_crane(request, pk):
    """Delete a crane row and all related records from the database."""
    if request.method != 'POST':
        return redirect('update_rg')

    crane = get_object_or_404(Crane, pk=pk)
    redirect_url = request.POST.get('next') or '/update_rg/'

    _log_change(
        request,
        'delete_crane',
        crane=crane,
        details='Crane row deleted permanently from database.',
    )

    # Remove related termination history first to satisfy FK protection.
    Termination.objects.filter(crane=crane).delete()
    crane.delete()
    return redirect(redirect_url)


@login_required(login_url='login')
@never_cache
def terminations_list(request):
    """Display all early-termination records, newest first."""
    queryset = Termination.objects.select_related('crane', 'terminated_by').order_by('-terminated_at')
    paginator = Paginator(queryset, 10)
    page_obj = paginator.get_page(request.GET.get('page'))
    return render(request, 'terminations.html', {'page_obj': page_obj})


@login_required(login_url='login')
@never_cache
def history_list(request):
    queryset = ChangeHistory.objects.select_related('changed_by', 'crane').all()

    paginator = Paginator(queryset, 20)
    page_obj = paginator.get_page(request.GET.get('page'))

    return render(request, 'history.html', {'page_obj': page_obj})