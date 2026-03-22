import csv
import json
from collections import Counter
from datetime import date, datetime

from dateutil.relativedelta import relativedelta
from django.conf import settings
from django.contrib import messages
from django.contrib.auth import authenticate, login as auth_login, logout
from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator
from django.db import transaction
from django.db.models import Count, Exists, OuterRef
from django.http import HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import csrf_protect, ensure_csrf_cookie

from .models import ChangeHistory, Crane, CraneDueTracking, CranePaymentHistory, Termination


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


def _attach_expiry_flag(cranes):
    """No expiry logic - dates are just reference numbers."""
    for crane in cranes:
        crane.is_expired = False


def _due_display_date_for_crane(crane):
    due_status = getattr(crane, 'due_status', None)
    tracked_due = _parse_iso_date(due_status.next_due_date) if due_status else None
    if tracked_due:
        return tracked_due.strftime('%Y-%m-%d')
    return crane.bezahlt_bis_rg_erstellt


def _due_years_for_crane(crane):
    due_date = _current_due_date_for_crane(crane)

    if not due_date:
        return []

    return [due_date.year]


def _current_due_date_for_crane(crane):
    initial_due_date = _parse_iso_date(crane.bezahlt_bis_rg_erstellt)

    if not initial_due_date:
        return None

    tracked_due_str = None
    due_status = getattr(crane, 'due_status', None)
    if due_status:
        tracked_due_str = due_status.next_due_date

    tracked_due = _parse_iso_date(tracked_due_str)
    if not tracked_due:
        return initial_due_date

    if tracked_due < initial_due_date:
        return initial_due_date

    return tracked_due


def _next_due_preview_date(current_due, expiry_date=None):
    if not current_due:
        return None

    return current_due + relativedelta(years=1)


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

    if not current_due:
        return None, None, None

    if actual_paid_date is None:
        actual_paid_date = timezone.localdate()

    server_today = min(timezone.localdate(), date.today())
    if actual_paid_date > server_today:
        return None, None, None

    next_due = _next_due_preview_date(current_due)
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

    return current_due, next_due, None


def _mark_due_unpaid_in_background(crane):
    initial_due_date = _parse_iso_date(crane.bezahlt_bis_rg_erstellt)

    if not initial_due_date:
        return None, None, None

    due_status = getattr(crane, 'due_status', None)
    tracked_due = _parse_iso_date(due_status.next_due_date) if due_status else None

    if not tracked_due:
        return None, None, None

    previous_due = tracked_due + relativedelta(years=-1)
    if previous_due < initial_due_date:
        return None, None, None

    with transaction.atomic():
        latest_payment = due_status.payment_history.order_by('-recorded_at', '-id').first()
        if latest_payment:
            latest_payment.delete()

        due_status.next_due_date = previous_due.strftime('%Y-%m-%d')
        _restore_due_status_payment_snapshot(due_status)
        due_status.save(update_fields=['next_due_date', 'last_paid_at', 'actual_paid_date'])

    return previous_due, initial_due_date, None


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
    initial_due_date = _parse_iso_date(crane.bezahlt_bis_rg_erstellt)

    if not initial_due_date:
        return []

    inferred_last_paid_due = initial_due_date + relativedelta(years=-1)

    current_due = _current_due_date_for_crane(crane)
    last_paid_due = current_due + relativedelta(years=-1) if current_due else inferred_last_paid_due

    if last_paid_due < inferred_last_paid_due:
        return []

    paid_dates = []
    cursor = inferred_last_paid_due
    while cursor <= last_paid_due:
        paid_dates.append(cursor)
        cursor = cursor + relativedelta(years=1)

    return paid_dates


def _paid_years_for_crane(crane):
    return [due_date.year for due_date in _paid_due_dates_for_crane(crane)]


def _last_paid_year_for_crane(crane):
    initial_due_date = _parse_iso_date(crane.bezahlt_bis_rg_erstellt)
    if not initial_due_date:
        return None

    due_status = getattr(crane, 'due_status', None)
    if not due_status:
        return (initial_due_date + relativedelta(years=-1)).year

    if due_status.actual_paid_date:
        return due_status.actual_paid_date.year

    if due_status.last_paid_at:
        return due_status.last_paid_at.year

    return (initial_due_date + relativedelta(years=-1)).year


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

    queryset = list(Crane.objects.filter(is_active=True).select_related('due_status').order_by('id'))
    queryset = [
        crane for crane in queryset
        if _matches_due_filter(crane, year, month_num, day_num)
    ]

    today_value = date.today()

    for crane in queryset:
        current_due = _current_due_date_for_crane(crane)
        expiry_date = _parse_iso_date(crane.bezahlt_bis_rg_erstellt)
        next_due_preview = _next_due_preview_date(current_due, expiry_date)
        display_date = _due_display_date_for_crane(crane)
        display_date_parsed = _parse_iso_date(display_date)

        crane.current_due_date = current_due
        crane.next_due_preview = next_due_preview
        crane.is_due_overdue = False
        crane.display_due_date = display_date
        crane.warning_due_date = bool(display_date_parsed and display_date_parsed <= today_value)
        crane.last_paid_year = _last_paid_year_for_crane(crane)

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

    queryset = list(Crane.objects.filter(is_active=True).select_related('due_status').order_by('id'))
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

    for crane in queryset:
        crane.display_due_date = _due_display_date_for_crane(crane)
        crane.last_paid_year = _last_paid_year_for_crane(crane)

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
        'today_iso': timezone.localdate().isoformat(),
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
    today_value = date.today()
    growth_window = request.GET.get('growth_window', 'all').strip().lower()
    if growth_window not in {'all', '1m', '3m', '6m'}:
        growth_window = 'all'

    growth_year = request.GET.get('growth_year', 'all').strip()

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

    customer_counts = list(
        Crane.objects.exclude(kunde__isnull=True)
        .exclude(kunde__exact='')
        .values('kunde')
        .annotate(total=Count('id'))
        .order_by('-total', 'kunde')[:10]
    )

    growth_records = []
    growth_years = set()
    growth_queryset = Crane.objects.all().only('id', 'kunde', 'amount', 'lizenzdatum', 'rueckmeldung')
    for crane in growth_queryset:
        anchor_date = _parse_iso_date(crane.lizenzdatum) or _parse_iso_date(crane.rueckmeldung)
        if not anchor_date:
            continue

        amount_value = crane.amount or 0
        growth_records.append({
            'date': anchor_date,
            'amount': amount_value,
            'kunde': crane.kunde or '',
        })
        growth_years.add(anchor_date.year)

    growth_year_options = [str(year_value) for year_value in sorted(growth_years, reverse=True)]
    growth_scope_label = 'All Years'
    filtered_growth_records = list(growth_records)
    growth_labels = []
    growth_amount_values = []
    growth_count_values = []
    selected_growth_year = 'all'
    selected_growth_window = 'all'

    if growth_year.isdigit() and int(growth_year) in growth_years:
        selected_year_value = int(growth_year)
        selected_growth_year = str(selected_year_value)
        filtered_growth_records = [
            row for row in growth_records
            if row['date'].year == selected_year_value
        ]

        monthly_buckets = {
            month_value: {
                'label': date(selected_year_value, month_value, 1).strftime('%b'),
                'amount': 0,
                'count': 0,
            }
            for month_value in range(1, 13)
        }

        for row in filtered_growth_records:
            monthly_buckets[row['date'].month]['amount'] += row['amount']
            monthly_buckets[row['date'].month]['count'] += 1

        growth_scope_label = f'Year {selected_year_value}'
        growth_labels = [bucket['label'] for bucket in monthly_buckets.values()]
        growth_amount_values = [bucket['amount'] for bucket in monthly_buckets.values()]
        growth_count_values = [bucket['count'] for bucket in monthly_buckets.values()]
    elif growth_window != 'all':
        selected_growth_window = growth_window
        months_back = {'1m': 1, '3m': 3, '6m': 6}[growth_window]
        range_start = today_value - relativedelta(months=months_back)
        filtered_growth_records = [
            row for row in growth_records
            if range_start <= row['date'] <= today_value
        ]

        month_cursor = date(range_start.year, range_start.month, 1)
        month_limit = date(today_value.year, today_value.month, 1)
        monthly_buckets = {}
        while month_cursor <= month_limit:
            monthly_buckets[month_cursor] = {
                'label': month_cursor.strftime('%b %Y'),
                'amount': 0,
                'count': 0,
            }
            month_cursor = month_cursor + relativedelta(months=1)

        for row in filtered_growth_records:
            bucket_key = date(row['date'].year, row['date'].month, 1)
            if bucket_key in monthly_buckets:
                monthly_buckets[bucket_key]['amount'] += row['amount']
                monthly_buckets[bucket_key]['count'] += 1

        growth_scope_label = {
            '1m': 'Last 1 Month',
            '3m': 'Last 3 Months',
            '6m': 'Last 6 Months',
        }[growth_window]
        growth_labels = [bucket['label'] for bucket in monthly_buckets.values()]
        growth_amount_values = [bucket['amount'] for bucket in monthly_buckets.values()]
        growth_count_values = [bucket['count'] for bucket in monthly_buckets.values()]
    else:
        yearly_buckets = {
            year_value: {
                'label': str(year_value),
                'amount': 0,
                'count': 0,
            }
            for year_value in sorted(growth_years)
        }

        for row in filtered_growth_records:
            yearly_buckets[row['date'].year]['amount'] += row['amount']
            yearly_buckets[row['date'].year]['count'] += 1

        growth_labels = [bucket['label'] for bucket in yearly_buckets.values()]
        growth_amount_values = [bucket['amount'] for bucket in yearly_buckets.values()]
        growth_count_values = [bucket['count'] for bucket in yearly_buckets.values()]

    growth_total_amount = sum(row['amount'] for row in filtered_growth_records)
    growth_total_cranes = len(filtered_growth_records)
    growth_unique_customers = len({row['kunde'] for row in filtered_growth_records if row['kunde']})
    growth_average_amount = round(growth_total_amount / growth_total_cranes) if growth_total_cranes else 0

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
    amount_growth_chart = {
        'labels': growth_labels,
        'amount_values': growth_amount_values,
        'count_values': growth_count_values,
    }
    customer_chart = {
        'labels': [entry['kunde'] for entry in customer_counts],
        'values': [entry['total'] for entry in customer_counts],
    }

    if request.headers.get('x-requested-with') == 'XMLHttpRequest':
        return JsonResponse({
            'amount_growth_chart': amount_growth_chart,
            'growth_scope_label': growth_scope_label,
            'growth_total_amount': growth_total_amount,
            'growth_total_cranes': growth_total_cranes,
            'growth_unique_customers': growth_unique_customers,
            'growth_average_amount': growth_average_amount,
            'selected_growth_year': selected_growth_year,
            'selected_growth_window': selected_growth_window,
        })

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
            "amount_growth_chart": amount_growth_chart,
            "customer_chart": customer_chart,
            "growth_year_options": growth_year_options,
            "selected_growth_year": selected_growth_year,
            "selected_growth_window": selected_growth_window,
            "growth_scope_label": growth_scope_label,
            "growth_total_amount": growth_total_amount,
            "growth_total_cranes": growth_total_cranes,
            "growth_unique_customers": growth_unique_customers,
            "growth_average_amount": growth_average_amount,
        },
    )

@login_required(login_url='login')
@never_cache
def data(request):
    queryset = list(_with_termination_flag(Crane.objects.select_related('due_status').all()).order_by('id'))
    _attach_expiry_flag(queryset)
    today_value = date.today()

    major_search_fields = [
        ('kunde', 'Kunde'),
        ('kran_typ', 'Kran Typ'),
        ('fabrik_nr', 'Fabrik Nr'),
        ('serien_nr', 'Serien Nr'),
        ('it_nr', 'IT Nr'),
        ('status', 'Status'),
    ]
    allowed_primary_fields = {field for field, _ in major_search_fields}

    primary_field = request.GET.get('primary_field', 'kunde').strip()
    if primary_field not in allowed_primary_fields:
        primary_field = 'kunde'

    primary_value = request.GET.get('primary_value', '').strip()
    ref_kran_typ = request.GET.get('ref_kran_typ', '').strip()
    ref_status = request.GET.get('ref_status', '').strip().lower()
    if ref_status not in {'', 'active', 'inactive', 'terminated'}:
        ref_status = ''

    ref_lizenz_year = request.GET.get('ref_lizenz_year', '').strip()
    if ref_lizenz_year and (not ref_lizenz_year.isdigit() or len(ref_lizenz_year) != 4):
        ref_lizenz_year = ''

    ref_amount_bucket = request.GET.get('ref_amount_bucket', '').strip()
    if ref_amount_bucket not in {'', '0_999', '1000_2499', '2500_4999', '5000_plus'}:
        ref_amount_bucket = ''

    ref_last_paid_year = request.GET.get('ref_last_paid_year', '').strip().lower()
    if ref_last_paid_year != 'null' and (ref_last_paid_year and (not ref_last_paid_year.isdigit() or len(ref_last_paid_year) != 4)):
        ref_last_paid_year = ''

    ref_lg = request.GET.get('ref_lg', '').strip()

    page_size_raw = request.GET.get('page_size', '10').strip()
    try:
        page_size = int(page_size_raw)
    except (TypeError, ValueError):
        page_size = 10

    if page_size not in (10, 50, 100):
        page_size = 10

    def _status_value(crane):
        if getattr(crane, 'is_terminated', False):
            return 'terminated'
        return 'active' if crane.is_active else 'inactive'

    def _lizenz_year_value(crane):
        parsed_date = _parse_iso_date(crane.lizenzdatum)
        return parsed_date.year if parsed_date else None

    def _matches_amount_bucket(crane, bucket):
        amount_value = crane.amount if crane.amount is not None else 0
        if bucket == '0_999':
            return 0 <= amount_value <= 999
        if bucket == '1000_2499':
            return 1000 <= amount_value <= 2499
        if bucket == '2500_4999':
            return 2500 <= amount_value <= 4999
        if bucket == '5000_plus':
            return amount_value >= 5000
        return True

    def _last_paid_year_value(crane):
        return _last_paid_year_for_crane(crane)

    def _apply_primary_filter(records):
        if not primary_value:
            return list(records)

        needle = primary_value.lower()

        if primary_field == 'status':
            return [
                crane for crane in records
                if _status_value(crane) == needle
            ]

        return [
            crane for crane in records
            if needle in str(getattr(crane, primary_field, '') or '').lower()
        ]

    def _apply_refinements(records, skip=None):
        filtered_records = list(records)

        if ref_kran_typ and skip != 'ref_kran_typ':
            filtered_records = [
                crane for crane in filtered_records
                if (crane.kran_typ or '').strip() == ref_kran_typ
            ]

        if ref_status and skip != 'ref_status':
            filtered_records = [
                crane for crane in filtered_records
                if _status_value(crane) == ref_status
            ]

        if ref_lizenz_year and skip != 'ref_lizenz_year':
            selected_year = int(ref_lizenz_year)
            filtered_records = [
                crane for crane in filtered_records
                if _lizenz_year_value(crane) == selected_year
            ]

        if ref_amount_bucket and skip != 'ref_amount_bucket':
            filtered_records = [
                crane for crane in filtered_records
                if _matches_amount_bucket(crane, ref_amount_bucket)
            ]

        if ref_last_paid_year and skip != 'ref_last_paid_year':
            if ref_last_paid_year == 'null':
                filtered_records = [
                    crane for crane in filtered_records
                    if _last_paid_year_value(crane) is None
                ]
            else:
                selected_paid_year = int(ref_last_paid_year)
                filtered_records = [
                    crane for crane in filtered_records
                    if _last_paid_year_value(crane) == selected_paid_year
                ]

        if ref_lg and skip != 'ref_lg':
            filtered_records = [
                crane for crane in filtered_records
                if (crane.lg or '').strip().lower() == ref_lg.lower()
            ]

        return filtered_records

    def _build_query(updates=None, remove_keys=None):
        params = request.GET.copy()
        params.pop('export', None)
        params.pop('page', None)
        if 'page_size' not in params:
            params['page_size'] = str(page_size)

        if remove_keys:
            for key in remove_keys:
                params.pop(key, None)

        if updates:
            for key, value in updates.items():
                if value is None or str(value).strip() == '':
                    params.pop(key, None)
                else:
                    params[key] = str(value)

        query_string = params.urlencode()
        return f'?{query_string}' if query_string else '?'

    total_records = len(queryset)
    primary_filtered_records = _apply_primary_filter(queryset)
    filtered_records = _apply_refinements(primary_filtered_records)

    customer_name_counts = Counter(
        (crane.kunde or '').strip()
        for crane in primary_filtered_records
        if (crane.kunde or '').strip()
    )
    if primary_field == 'kunde' and primary_value:
        customer_suggestions = [
            customer_name
            for customer_name, _ in customer_name_counts.most_common()
            if primary_value.lower() in customer_name.lower()
        ][:12]
    else:
        customer_suggestions = [
            customer_name
            for customer_name, _ in customer_name_counts.most_common(12)
        ]

    def _facet_option(value, label, count, selected_value, param_name):
        return {
            'value': str(value),
            'label': label,
            'count': count,
            'active': str(selected_value) == str(value),
            'url': _build_query({param_name: value}),
        }

    kran_typ_source = _apply_refinements(primary_filtered_records, skip='ref_kran_typ')
    kran_typ_counts = Counter(
        (crane.kran_typ or '').strip()
        for crane in kran_typ_source
        if (crane.kran_typ or '').strip()
    )
    facet_kran_typ_options = [
        _facet_option(value, value, count, ref_kran_typ, 'ref_kran_typ')
        for value, count in kran_typ_counts.most_common(25)
    ]

    status_source = _apply_refinements(primary_filtered_records, skip='ref_status')
    status_counts = Counter(_status_value(crane) for crane in status_source)
    status_labels = {
        'active': 'Active',
        'inactive': 'Inactive',
        'terminated': 'Terminated',
    }
    facet_status_options = [
        _facet_option(status_key, status_labels[status_key], status_counts.get(status_key, 0), ref_status, 'ref_status')
        for status_key in ('active', 'inactive', 'terminated')
        if status_counts.get(status_key, 0) or ref_status == status_key
    ]

    lizenz_year_source = _apply_refinements(primary_filtered_records, skip='ref_lizenz_year')
    lizenz_year_counts = Counter(
        _lizenz_year_value(crane)
        for crane in lizenz_year_source
        if _lizenz_year_value(crane) is not None
    )
    facet_lizenz_year_options = [
        _facet_option(year_value, str(year_value), lizenz_year_counts.get(year_value, 0), ref_lizenz_year, 'ref_lizenz_year')
        for year_value in sorted(lizenz_year_counts.keys(), reverse=True)
    ]

    amount_bucket_labels = {
        '0_999': 'EUR 0 - 999',
        '1000_2499': 'EUR 1000 - 2499',
        '2500_4999': 'EUR 2500 - 4999',
        '5000_plus': 'EUR 5000+',
    }
    amount_source = _apply_refinements(primary_filtered_records, skip='ref_amount_bucket')
    amount_bucket_counts = Counter(
        bucket_key
        for bucket_key in ('0_999', '1000_2499', '2500_4999', '5000_plus')
        for crane in amount_source
        if _matches_amount_bucket(crane, bucket_key)
    )
    facet_amount_bucket_options = [
        _facet_option(bucket_key, amount_bucket_labels[bucket_key], amount_bucket_counts.get(bucket_key, 0), ref_amount_bucket, 'ref_amount_bucket')
        for bucket_key in ('0_999', '1000_2499', '2500_4999', '5000_plus')
        if amount_bucket_counts.get(bucket_key, 0) or ref_amount_bucket == bucket_key
    ]

    paid_year_source = _apply_refinements(primary_filtered_records, skip='ref_last_paid_year')
    paid_year_values = [_last_paid_year_value(crane) for crane in paid_year_source]
    paid_year_counts = Counter(year_value for year_value in paid_year_values if year_value is not None)
    null_paid_year_count = sum(1 for year_value in paid_year_values if year_value is None)
    facet_last_paid_year_options = [
        _facet_option(year_value, str(year_value), paid_year_counts.get(year_value, 0), ref_last_paid_year, 'ref_last_paid_year')
        for year_value in sorted(paid_year_counts.keys(), reverse=True)
    ]
    if null_paid_year_count or ref_last_paid_year == 'null':
        facet_last_paid_year_options.append(
            _facet_option('null', 'Null', null_paid_year_count, ref_last_paid_year, 'ref_last_paid_year')
        )

    lg_source = _apply_refinements(primary_filtered_records, skip='ref_lg')
    lg_counts = Counter(
        (crane.lg or '').strip()
        for crane in lg_source
        if (crane.lg or '').strip()
    )
    facet_lg_options = [
        _facet_option(value, value, count, ref_lg, 'ref_lg')
        for value, count in lg_counts.most_common(20)
    ]

    selected_filter_chips = []
    field_labels = dict(major_search_fields)

    if primary_value:
        selected_filter_chips.append({
            'label': f"{field_labels.get(primary_field, primary_field.title())}: {primary_value}",
            'remove_url': _build_query({'primary_value': None}),
        })

    if ref_kran_typ:
        selected_filter_chips.append({
            'label': f'Kran Typ: {ref_kran_typ}',
            'remove_url': _build_query({'ref_kran_typ': None}),
        })

    if ref_status:
        selected_filter_chips.append({
            'label': f"Status: {status_labels.get(ref_status, ref_status.title())}",
            'remove_url': _build_query({'ref_status': None}),
        })

    if ref_lizenz_year:
        selected_filter_chips.append({
            'label': f'Lizenz Year: {ref_lizenz_year}',
            'remove_url': _build_query({'ref_lizenz_year': None}),
        })

    if ref_amount_bucket:
        selected_filter_chips.append({
            'label': f"Amount: {amount_bucket_labels.get(ref_amount_bucket, ref_amount_bucket)}",
            'remove_url': _build_query({'ref_amount_bucket': None}),
        })

    if ref_last_paid_year:
        selected_filter_chips.append({
            'label': f"Last Paid Year: {ref_last_paid_year.upper() if ref_last_paid_year == 'null' else ref_last_paid_year}",
            'remove_url': _build_query({'ref_last_paid_year': None}),
        })

    if ref_lg:
        selected_filter_chips.append({
            'label': f'LG: {ref_lg}',
            'remove_url': _build_query({'ref_lg': None}),
        })

    has_refinements = bool(ref_kran_typ or ref_status or ref_lizenz_year or ref_amount_bucket or ref_last_paid_year or ref_lg)

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

        for crane in filtered_records:
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
    paginator = Paginator(filtered_records, page_size)
    page_obj = paginator.get_page(request.GET.get('page'))
    _attach_expiry_flag(page_obj.object_list)
    for crane in page_obj.object_list:
        display_date = _due_display_date_for_crane(crane)
        display_date_parsed = _parse_iso_date(display_date)
        crane.display_due_date = display_date
        crane.warning_due_date = bool(display_date_parsed and display_date_parsed <= today_value)
        crane.last_paid_year = _last_paid_year_for_crane(crane)

    export_params = request.GET.copy()
    export_params.pop('page', None)
    export_params['page_size'] = str(page_size)
    export_params['export'] = 'true'
    export_url = '?' + export_params.urlencode()

    page_url_first = _build_query({'page': 1})
    page_url_prev = _build_query({'page': page_obj.previous_page_number()}) if page_obj.has_previous() else ''
    page_url_next = _build_query({'page': page_obj.next_page_number()}) if page_obj.has_next() else ''
    page_url_last = _build_query({'page': page_obj.paginator.num_pages}) if page_obj.has_next() else ''

    showing_start = page_obj.start_index() if paginator.count else 0
    showing_end = page_obj.end_index() if paginator.count else 0

    reset_url = f'?page_size={page_size}'

    return render(request, 'data_retrival.html', {
        'page_obj': page_obj,
        'page_size': page_size,
        'page_size_options': (10, 50, 100),
        'primary_field': primary_field,
        'primary_value': primary_value,
        'major_search_fields': major_search_fields,
        'ref_kran_typ': ref_kran_typ,
        'ref_status': ref_status,
        'ref_lizenz_year': ref_lizenz_year,
        'ref_amount_bucket': ref_amount_bucket,
        'ref_last_paid_year': ref_last_paid_year,
        'ref_lg': ref_lg,
        'facet_kran_typ_options': facet_kran_typ_options,
        'facet_status_options': facet_status_options,
        'facet_lizenz_year_options': facet_lizenz_year_options,
        'facet_amount_bucket_options': facet_amount_bucket_options,
        'facet_last_paid_year_options': facet_last_paid_year_options,
        'facet_lg_options': facet_lg_options,
        'customer_suggestions': customer_suggestions,
        'selected_filter_chips': selected_filter_chips,
        'has_refinements': has_refinements,
        'filtered_total': len(filtered_records),
        'total_records': total_records,
        'showing_start': showing_start,
        'showing_end': showing_end,
        'export_url': export_url,
        'page_url_first': page_url_first,
        'page_url_prev': page_url_prev,
        'page_url_next': page_url_next,
        'page_url_last': page_url_last,
        'reset_url': reset_url,
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

    if request.method == "POST" and kran.bezahlt_bis_rg_erstellt:
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
            }
        )
    
    return JsonResponse({'success': False, 'error': 'Invalid request'}, status=400)

@login_required(login_url='login')
@never_cache
def search_rg(request):
    """Search cranes by computed due years and mark selected due as paid."""
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
                pk=int(crane_id)
            )
            current_due = _current_due_date_for_crane(crane)

            if not current_due:
                messages.error(
                    request,
                    f'ID {crane.id}: invalid Bezahlt bis Rg.erstellt date.'
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

            server_today = min(timezone.localdate(), date.today())
            client_today_str = request.POST.get('client_today', '').strip()
            client_today = None
            if client_today_str:
                try:
                    client_today = datetime.strptime(client_today_str, '%Y-%m-%d').date()
                except ValueError:
                    client_today = None

            max_allowed_date = min(server_today, client_today) if client_today else server_today

            if actual_paid_date > max_allowed_date:
                messages.error(
                    request,
                    f'ID {crane.id}: actual payment date cannot be in the future.'
                )
                return redirect(redirect_url)

            _, next_due, _ = _mark_due_paid_in_background(crane, actual_paid_date=actual_paid_date)
            if not next_due:
                messages.error(request, f'ID {crane.id}: payment was not applied. Future dates are not allowed.')
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
                        'Last Paid Year', 'Servicemeldung', 'Amount', 'Status'])
        
        for crane in queryset:
            writer.writerow([
                crane.id, crane.kran_typ, crane.fabrik_nr, crane.kunde,
                crane.lg, crane.kundenummer, crane.version, crane.serien_nr,
                crane.tel_nr, crane.ip, crane.rueckmeldung,
                crane.it_nr, crane.kundenkran, crane.lizenz_ja,
                crane.lizenzdatum, getattr(crane, 'display_due_date', crane.bezahlt_bis_rg_erstellt),
                getattr(crane, 'last_paid_year', None),
                crane.servicemeldung,
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
                pk=int(crane_id)
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
                        'Last Paid Year', 'Servicemeldung', 'Amount', 'Status', 'Payment Date (Actual)', 'Recorded At (System)'])

        for crane in queryset:
            due = getattr(crane, 'due_status', None)
            actual_paid = str(due.actual_paid_date) if due and due.actual_paid_date else ''
            recorded_at = due.last_paid_at.strftime('%Y-%m-%d %H:%M:%S') if due and due.last_paid_at else ''
            writer.writerow([
                crane.id, crane.kran_typ, crane.fabrik_nr, crane.kunde,
                crane.lg, crane.kundenummer, crane.version, crane.serien_nr,
                crane.tel_nr, crane.ip, crane.rueckmeldung,
                crane.it_nr, crane.kundenkran, crane.lizenz_ja,
                crane.lizenzdatum, getattr(crane, 'display_due_date', crane.bezahlt_bis_rg_erstellt), getattr(crane, 'last_paid_year', None), crane.servicemeldung,
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
    """Display rows and support due-status workflows on update page."""

    if request.method == 'POST':
        action = request.POST.get('action', '').strip()
        crane_id = request.POST.get('crane_id', '').strip()
        redirect_url = request.POST.get('next') or '/update_rg/'

        if not crane_id.isdigit():
            messages.error(request, 'Invalid row selected.')
            return redirect(redirect_url)

        crane = get_object_or_404(Crane, pk=int(crane_id))

        if action == 'mark_paid':
            current_due = _current_due_date_for_crane(crane)

            if not current_due:
                messages.error(
                    request,
                    f'ID {crane.id}: invalid Bezahlt bis Rg.erstellt date.'
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

            server_today = min(timezone.localdate(), date.today())
            client_today_str = request.POST.get('client_today', '').strip()
            client_today = None
            if client_today_str:
                try:
                    client_today = datetime.strptime(client_today_str, '%Y-%m-%d').date()
                except ValueError:
                    client_today = None

            max_allowed_date = min(server_today, client_today) if client_today else server_today

            if actual_paid_date > max_allowed_date:
                messages.error(
                    request,
                    f'ID {crane.id}: actual payment date cannot be in the future.'
                )
                return redirect(redirect_url)

            _, next_due, _ = _mark_due_paid_in_background(crane, actual_paid_date=actual_paid_date)
            if not next_due:
                messages.error(request, f'ID {crane.id}: payment was not applied. Future dates are not allowed.')
                return redirect(redirect_url)

            _log_change(
                request,
                'mark_paid',
                crane=crane,
                details=f'Payment marked from update page with actual payment date {actual_paid_date}. Next due moved to {next_due}.',
            )
            return redirect(redirect_url)

        messages.error(request, 'Unsupported action.')
        return redirect(redirect_url)

    queryset = list(_with_termination_flag(Crane.objects.select_related('due_status').all()).order_by('id'))
    _attach_expiry_flag(queryset)
    today_value = date.today()
    for crane in queryset:
        display_date = _due_display_date_for_crane(crane)
        display_date_parsed = _parse_iso_date(display_date)
        crane.display_due_date = display_date
        crane.warning_due_date = bool(display_date_parsed and display_date_parsed <= today_value)
        crane.last_paid_year = _last_paid_year_for_crane(crane)

    major_search_fields = [
        ('kunde', 'Kunde'),
        ('kran_typ', 'Kran Typ'),
        ('fabrik_nr', 'Fabrik Nr'),
        ('serien_nr', 'Serien Nr'),
        ('it_nr', 'IT Nr'),
        ('status', 'Status'),
    ]
    allowed_primary_fields = {field for field, _ in major_search_fields}

    primary_field = request.GET.get('primary_field', 'kunde').strip()
    if primary_field not in allowed_primary_fields:
        primary_field = 'kunde'

    primary_value = request.GET.get('primary_value', '').strip()
    legacy_query = request.GET.get('q', '').strip()
    if legacy_query and not primary_value:
        primary_value = legacy_query

    ref_kran_typ = request.GET.get('ref_kran_typ', '').strip()
    ref_status = request.GET.get('ref_status', '').strip().lower()
    if ref_status not in {'', 'active', 'inactive', 'terminated'}:
        ref_status = ''

    ref_lizenz_year = request.GET.get('ref_lizenz_year', '').strip()
    if ref_lizenz_year and (not ref_lizenz_year.isdigit() or len(ref_lizenz_year) != 4):
        ref_lizenz_year = ''

    ref_amount_bucket = request.GET.get('ref_amount_bucket', '').strip()
    if ref_amount_bucket not in {'', '0_999', '1000_2499', '2500_4999', '5000_plus'}:
        ref_amount_bucket = ''

    ref_last_paid_year = request.GET.get('ref_last_paid_year', '').strip().lower()
    if ref_last_paid_year != 'null' and (ref_last_paid_year and (not ref_last_paid_year.isdigit() or len(ref_last_paid_year) != 4)):
        ref_last_paid_year = ''

    ref_lg = request.GET.get('ref_lg', '').strip()

    page_size_raw = request.GET.get('page_size', '10').strip()
    try:
        page_size = int(page_size_raw)
    except (TypeError, ValueError):
        page_size = 10

    if page_size not in (10, 50, 100):
        page_size = 10

    def _status_value(crane):
        if getattr(crane, 'is_terminated', False):
            return 'terminated'
        return 'active' if crane.is_active else 'inactive'

    def _lizenz_year_value(crane):
        parsed_date = _parse_iso_date(crane.lizenzdatum)
        return parsed_date.year if parsed_date else None

    def _matches_amount_bucket(crane, bucket):
        amount_value = crane.amount if crane.amount is not None else 0
        if bucket == '0_999':
            return 0 <= amount_value <= 999
        if bucket == '1000_2499':
            return 1000 <= amount_value <= 2499
        if bucket == '2500_4999':
            return 2500 <= amount_value <= 4999
        if bucket == '5000_plus':
            return amount_value >= 5000
        return True

    def _last_paid_year_value(crane):
        return _last_paid_year_for_crane(crane)

    def _apply_primary_filter(records):
        if not primary_value:
            return list(records)

        needle = primary_value.lower()

        if primary_field == 'status':
            return [
                crane for crane in records
                if _status_value(crane) == needle
            ]

        return [
            crane for crane in records
            if needle in str(getattr(crane, primary_field, '') or '').lower()
        ]

    def _apply_refinements(records, skip=None):
        filtered_records = list(records)

        if ref_kran_typ and skip != 'ref_kran_typ':
            filtered_records = [
                crane for crane in filtered_records
                if (crane.kran_typ or '').strip() == ref_kran_typ
            ]

        if ref_status and skip != 'ref_status':
            filtered_records = [
                crane for crane in filtered_records
                if _status_value(crane) == ref_status
            ]

        if ref_lizenz_year and skip != 'ref_lizenz_year':
            selected_year = int(ref_lizenz_year)
            filtered_records = [
                crane for crane in filtered_records
                if _lizenz_year_value(crane) == selected_year
            ]

        if ref_amount_bucket and skip != 'ref_amount_bucket':
            filtered_records = [
                crane for crane in filtered_records
                if _matches_amount_bucket(crane, ref_amount_bucket)
            ]

        if ref_last_paid_year and skip != 'ref_last_paid_year':
            if ref_last_paid_year == 'null':
                filtered_records = [
                    crane for crane in filtered_records
                    if _last_paid_year_value(crane) is None
                ]
            else:
                selected_paid_year = int(ref_last_paid_year)
                filtered_records = [
                    crane for crane in filtered_records
                    if _last_paid_year_value(crane) == selected_paid_year
                ]

        if ref_lg and skip != 'ref_lg':
            filtered_records = [
                crane for crane in filtered_records
                if (crane.lg or '').strip().lower() == ref_lg.lower()
            ]

        return filtered_records

    def _build_query(updates=None, remove_keys=None):
        params = request.GET.copy()
        params.pop('page', None)
        params.pop('q', None)
        if 'page_size' not in params:
            params['page_size'] = str(page_size)

        if remove_keys:
            for key in remove_keys:
                params.pop(key, None)

        if updates:
            for key, value in updates.items():
                if value is None or str(value).strip() == '':
                    params.pop(key, None)
                else:
                    params[key] = str(value)

        query_string = params.urlencode()
        return f'?{query_string}' if query_string else '?'

    total_records = len(queryset)
    primary_filtered_records = _apply_primary_filter(queryset)
    filtered_records = _apply_refinements(primary_filtered_records)

    customer_name_counts = Counter(
        (crane.kunde or '').strip()
        for crane in primary_filtered_records
        if (crane.kunde or '').strip()
    )
    if primary_field == 'kunde' and primary_value:
        customer_suggestions = [
            customer_name
            for customer_name, _ in customer_name_counts.most_common()
            if primary_value.lower() in customer_name.lower()
        ][:12]
    else:
        customer_suggestions = [
            customer_name
            for customer_name, _ in customer_name_counts.most_common(12)
        ]

    def _facet_option(value, label, count, selected_value, param_name):
        return {
            'value': str(value),
            'label': label,
            'count': count,
            'active': str(selected_value) == str(value),
            'url': _build_query({param_name: value}),
        }

    kran_typ_source = _apply_refinements(primary_filtered_records, skip='ref_kran_typ')
    kran_typ_counts = Counter(
        (crane.kran_typ or '').strip()
        for crane in kran_typ_source
        if (crane.kran_typ or '').strip()
    )
    facet_kran_typ_options = [
        _facet_option(value, value, count, ref_kran_typ, 'ref_kran_typ')
        for value, count in kran_typ_counts.most_common(25)
    ]

    status_source = _apply_refinements(primary_filtered_records, skip='ref_status')
    status_counts = Counter(_status_value(crane) for crane in status_source)
    status_labels = {
        'active': 'Active',
        'inactive': 'Inactive',
        'terminated': 'Terminated',
    }
    facet_status_options = [
        _facet_option(status_key, status_labels[status_key], status_counts.get(status_key, 0), ref_status, 'ref_status')
        for status_key in ('active', 'inactive', 'terminated')
        if status_counts.get(status_key, 0) or ref_status == status_key
    ]

    lizenz_year_source = _apply_refinements(primary_filtered_records, skip='ref_lizenz_year')
    lizenz_year_counts = Counter(
        _lizenz_year_value(crane)
        for crane in lizenz_year_source
        if _lizenz_year_value(crane) is not None
    )
    facet_lizenz_year_options = [
        _facet_option(year_value, str(year_value), lizenz_year_counts.get(year_value, 0), ref_lizenz_year, 'ref_lizenz_year')
        for year_value in sorted(lizenz_year_counts.keys(), reverse=True)
    ]

    amount_bucket_labels = {
        '0_999': 'EUR 0 - 999',
        '1000_2499': 'EUR 1000 - 2499',
        '2500_4999': 'EUR 2500 - 4999',
        '5000_plus': 'EUR 5000+',
    }
    amount_source = _apply_refinements(primary_filtered_records, skip='ref_amount_bucket')
    amount_bucket_counts = Counter(
        bucket_key
        for bucket_key in ('0_999', '1000_2499', '2500_4999', '5000_plus')
        for crane in amount_source
        if _matches_amount_bucket(crane, bucket_key)
    )
    facet_amount_bucket_options = [
        _facet_option(bucket_key, amount_bucket_labels[bucket_key], amount_bucket_counts.get(bucket_key, 0), ref_amount_bucket, 'ref_amount_bucket')
        for bucket_key in ('0_999', '1000_2499', '2500_4999', '5000_plus')
        if amount_bucket_counts.get(bucket_key, 0) or ref_amount_bucket == bucket_key
    ]

    paid_year_source = _apply_refinements(primary_filtered_records, skip='ref_last_paid_year')
    paid_year_values = [_last_paid_year_value(crane) for crane in paid_year_source]
    paid_year_counts = Counter(year_value for year_value in paid_year_values if year_value is not None)
    null_paid_year_count = sum(1 for year_value in paid_year_values if year_value is None)
    facet_last_paid_year_options = [
        _facet_option(year_value, str(year_value), paid_year_counts.get(year_value, 0), ref_last_paid_year, 'ref_last_paid_year')
        for year_value in sorted(paid_year_counts.keys(), reverse=True)
    ]
    if null_paid_year_count or ref_last_paid_year == 'null':
        facet_last_paid_year_options.append(
            _facet_option('null', 'Null', null_paid_year_count, ref_last_paid_year, 'ref_last_paid_year')
        )

    lg_source = _apply_refinements(primary_filtered_records, skip='ref_lg')
    lg_counts = Counter(
        (crane.lg or '').strip()
        for crane in lg_source
        if (crane.lg or '').strip()
    )
    facet_lg_options = [
        _facet_option(value, value, count, ref_lg, 'ref_lg')
        for value, count in lg_counts.most_common(20)
    ]

    selected_filter_chips = []
    field_labels = dict(major_search_fields)

    if primary_value:
        selected_filter_chips.append({
            'label': f"{field_labels.get(primary_field, primary_field.title())}: {primary_value}",
            'remove_url': _build_query({'primary_value': None}),
        })

    if ref_kran_typ:
        selected_filter_chips.append({
            'label': f'Kran Typ: {ref_kran_typ}',
            'remove_url': _build_query({'ref_kran_typ': None}),
        })

    if ref_status:
        selected_filter_chips.append({
            'label': f"Status: {status_labels.get(ref_status, ref_status.title())}",
            'remove_url': _build_query({'ref_status': None}),
        })

    if ref_lizenz_year:
        selected_filter_chips.append({
            'label': f'Lizenz Year: {ref_lizenz_year}',
            'remove_url': _build_query({'ref_lizenz_year': None}),
        })

    if ref_amount_bucket:
        selected_filter_chips.append({
            'label': f"Amount: {amount_bucket_labels.get(ref_amount_bucket, ref_amount_bucket)}",
            'remove_url': _build_query({'ref_amount_bucket': None}),
        })

    if ref_last_paid_year:
        selected_filter_chips.append({
            'label': f"Last Paid Year: {ref_last_paid_year.upper() if ref_last_paid_year == 'null' else ref_last_paid_year}",
            'remove_url': _build_query({'ref_last_paid_year': None}),
        })

    if ref_lg:
        selected_filter_chips.append({
            'label': f'LG: {ref_lg}',
            'remove_url': _build_query({'ref_lg': None}),
        })

    has_refinements = bool(ref_kran_typ or ref_status or ref_lizenz_year or ref_amount_bucket or ref_last_paid_year or ref_lg)

    paginator = Paginator(filtered_records, page_size)
    page_obj = paginator.get_page(request.GET.get('page'))
    _attach_expiry_flag(page_obj.object_list)
    for crane in page_obj.object_list:
        display_date = _due_display_date_for_crane(crane)
        display_date_parsed = _parse_iso_date(display_date)
        crane.display_due_date = display_date
        crane.warning_due_date = bool(display_date_parsed and display_date_parsed <= today_value)

    page_url_first = _build_query({'page': 1})
    page_url_prev = _build_query({'page': page_obj.previous_page_number()}) if page_obj.has_previous() else ''
    page_url_next = _build_query({'page': page_obj.next_page_number()}) if page_obj.has_next() else ''
    page_url_last = _build_query({'page': page_obj.paginator.num_pages}) if page_obj.has_next() else ''

    showing_start = page_obj.start_index() if paginator.count else 0
    showing_end = page_obj.end_index() if paginator.count else 0

    reset_url = f'?page_size={page_size}'

    return render(request, 'update.html', {
        'page_obj': page_obj,
        'page_size': page_size,
        'page_size_options': (10, 50, 100),
        'primary_field': primary_field,
        'primary_value': primary_value,
        'major_search_fields': major_search_fields,
        'ref_kran_typ': ref_kran_typ,
        'ref_status': ref_status,
        'ref_lizenz_year': ref_lizenz_year,
        'ref_amount_bucket': ref_amount_bucket,
        'ref_last_paid_year': ref_last_paid_year,
        'ref_lg': ref_lg,
        'facet_kran_typ_options': facet_kran_typ_options,
        'facet_status_options': facet_status_options,
        'facet_lizenz_year_options': facet_lizenz_year_options,
        'facet_amount_bucket_options': facet_amount_bucket_options,
        'facet_last_paid_year_options': facet_last_paid_year_options,
        'facet_lg_options': facet_lg_options,
        'customer_suggestions': customer_suggestions,
        'selected_filter_chips': selected_filter_chips,
        'has_refinements': has_refinements,
        'filtered_total': len(filtered_records),
        'total_records': total_records,
        'showing_start': showing_start,
        'showing_end': showing_end,
        'page_url_first': page_url_first,
        'page_url_prev': page_url_prev,
        'page_url_next': page_url_next,
        'page_url_last': page_url_last,
        'reset_url': reset_url,
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
        error_msg = f'ID {crane.id}: already inactive — termination aborted.'
        if request.headers.get('Content-Type') == 'application/json':
            return JsonResponse({'success': False, 'error': error_msg}, status=400)
        messages.error(request, error_msg)
        return redirect('update_rg')

    # Handle JSON payload for AJAX
    reason = ''
    if request.headers.get('Content-Type') == 'application/json':
        try:
            data = json.loads(request.body)
            reason = data.get('reason', '').strip()
        except (json.JSONDecodeError, ValueError):
            reason = ''
    else:
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
    
    # Handle AJAX request
    if request.headers.get('Content-Type') == 'application/json':
        return JsonResponse({'success': True, 'is_active': crane.is_active})
    
    return redirect('update_rg')


@login_required(login_url='login')
@never_cache
def renew_termination(request, pk):
    """Renew a terminated crane by setting a new expiry date and removing termination status."""
    if request.method != 'POST':
        return redirect('terminations_list')

    termination = get_object_or_404(Termination.objects.select_related('crane'), pk=pk)
    crane = termination.crane
    new_expiry = request.POST.get('bezahlt_bis_rg_erstellt', '').strip()

    if not new_expiry:
        messages.error(request, f'ID {crane.id}: please provide a new Bezahlt bis Rg.erstellt date.')
        return redirect('terminations_list')

    try:
        parsed_expiry = datetime.strptime(new_expiry, '%Y-%m-%d').date()
    except ValueError:
        messages.error(request, f'ID {crane.id}: invalid date format. Use YYYY-MM-DD.')
        return redirect('terminations_list')

    today_value = timezone.localdate()
    if parsed_expiry < today_value:
        messages.error(request, f'ID {crane.id}: renewal date cannot be in the past.')
        return redirect('terminations_list')

    start_date = _parse_iso_date(crane.lizenzdatum or termination.original_lizenzdatum)
    if start_date and parsed_expiry <= start_date:
        messages.error(request, f'ID {crane.id}: renewal date must be after Lizenzdatum.')
        return redirect('terminations_list')

    current_due = _current_due_date_for_crane(crane)
    if current_due and parsed_expiry <= current_due:
        messages.error(request, f'ID {crane.id}: renewal date must be after the current due date {current_due}.')
        return redirect('terminations_list')

    crane.bezahlt_bis_rg_erstellt = parsed_expiry.strftime('%Y-%m-%d')
    crane.is_active = True
    crane.save(update_fields=['bezahlt_bis_rg_erstellt', 'is_active'])

    termination.delete()

    _log_change(
        request,
        'renew_crane',
        crane=crane,
        details=f'Lease renewed from terminated state. New Bezahlt bis Rg.erstellt set to {parsed_expiry}.',
    )

    return redirect('terminations_list')


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

    if request.GET.get('export') == 'true':
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="termination_records.csv"'

        writer = csv.writer(response)
        writer.writerow([
            'ID', 'Kran Typ', 'Fabrik Nr', 'Kunde', 'LG', 'Kundenummer',
            'Version', 'Serien Nr', 'Telefon', 'IP', 'Rueckmeldung', 'IT Nr',
            'Kundenkran', 'Lizenz Ja', 'Lizenzdatum', 'Bezahlt bis Rg.erstellt',
            'Servicemeldung', 'Amount', 'Status', 'Terminated At', 'Terminated By', 'Reason'
        ])

        for item in queryset:
            writer.writerow([
                item.crane.id,
                item.crane.kran_typ,
                item.crane.fabrik_nr,
                item.crane.kunde,
                item.crane.lg,
                item.crane.kundenummer,
                item.crane.version,
                item.crane.serien_nr,
                item.crane.tel_nr,
                item.crane.ip,
                item.crane.rueckmeldung,
                item.crane.it_nr,
                item.crane.kundenkran,
                item.crane.lizenz_ja,
                item.original_lizenzdatum,
                item.original_expiry_date,
                item.crane.servicemeldung,
                item.crane.amount,
                'Terminated',
                item.terminated_at.strftime('%Y-%m-%d %H:%M:%S') if item.terminated_at else '',
                item.terminated_by.username if item.terminated_by else '',
                item.termination_reason,
            ])

        return response

    paginator = Paginator(queryset, 10)
    page_obj = paginator.get_page(request.GET.get('page'))
    return render(request, 'terminations.html', {'page_obj': page_obj})


@login_required(login_url='login')
@never_cache
def history_list(request):
    queryset = ChangeHistory.objects.select_related('changed_by', 'crane').all()

    if request.GET.get('export') == 'true':
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="change_history.csv"'

        writer = csv.writer(response)
        writer.writerow(['Changed At', 'User', 'Action', 'Crane ID', 'Kunde', 'Details'])

        for item in queryset:
            writer.writerow([
                item.changed_at.strftime('%Y-%m-%d %H:%M:%S') if item.changed_at else '',
                item.changed_by.username if item.changed_by else 'System',
                item.action,
                item.crane_display_id,
                item.crane_kunde,
                item.details,
            ])

        return response

    paginator = Paginator(queryset, 20)
    page_obj = paginator.get_page(request.GET.get('page'))

    return render(request, 'history.html', {'page_obj': page_obj})