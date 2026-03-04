from django.shortcuts import render
from django.shortcuts import render, redirect
from django.contrib.auth.models import User
from django.contrib.auth import logout
from django.contrib import messages
from django.shortcuts import render
from .models import Crane   # change if your model name is different
from django.core.paginator import Paginator
from django.http import HttpResponse
import csv
from django.contrib.auth import authenticate, login as auth_login
from django.contrib import messages
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from django.views.decorators.cache import never_cache
from django.http import Http404
from django.views.decorators.csrf import csrf_protect, ensure_csrf_cookie
from dateutil.relativedelta import relativedelta
from .models import Crane, CraneDueTracking
from datetime import datetime
from datetime import date
from django.shortcuts import get_object_or_404


def _parse_iso_date(value):
    if not value:
        return None

    try:
        year_str, month_str, day_str = str(value).strip().split('-')
        return date(int(year_str), int(month_str), int(day_str))
    except (ValueError, TypeError, AttributeError):
        return None


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


def _mark_due_paid_in_background(crane):
    current_due = _current_due_date_for_crane(crane)
    expiry_date = _parse_iso_date(crane.bezahlt_bis_rg_erstellt)

    if not current_due or not expiry_date:
        return None, None, None

    next_due = current_due + relativedelta(years=1)
    if next_due > expiry_date:
        next_due = expiry_date

    due_status, _ = CraneDueTracking.objects.get_or_create(crane=crane)
    due_status.next_due_date = next_due.strftime('%Y-%m-%d')
    due_status.save(update_fields=['next_due_date'])

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

    due_status.next_due_date = previous_due.strftime('%Y-%m-%d')
    due_status.save(update_fields=['next_due_date'])

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


def _sort_value(crane, sort_by):
    value = getattr(crane, sort_by, None)

    if value is None:
        return ''

    if isinstance(value, (int, float, bool)):
        return value

    return str(value).lower()

@csrf_protect
@ensure_csrf_cookie
@never_cache
def login(request):
    if request.method == "POST":
        username = request.POST.get("username")
        password = request.POST.get("password")

        user = authenticate(request, username=username, password=password)

        if user is not None:
            auth_login(request, user)
            return redirect('analyst_dashboard')

        else:
            messages.error(request, "Invalid username or password")

    return render(request, "login.html")


@login_required(login_url='login')
@never_cache
def index(request):
    data = DepartmentData.objects.all()

    return render(request, "index.html", {
        "group": "All Users",
        "data": data
    })

@login_required(login_url='login')
@never_cache
def data(request):
    queryset = Crane.objects.all().order_by('id')

    # 📊 EXPORT CSV
    if request.GET.get('export') == 'true':
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="crane_data.csv"'

        writer = csv.writer(response)

        writer.writerow([
            'ID', 'Kran Typ', 'Fabrik Nr', 'Kunde', 'LG', 'Kundenummer',
            'Version', 'Serien Nr', 'Tel Nr', 'IP', 'Rueckmeldung',
            'IT Nr', 'Kundenkran', 'Lizenz Ja', 'Lizenzdatum',
            'Bezahlt bis Rg.erstellt', 'Servicemeldung', 'Amount'
        ])

        for crane in queryset:
            writer.writerow([
                crane.id, crane.kran_typ, crane.fabrik_nr, crane.kunde,
                crane.lg, crane.kundenummer, crane.version,
                crane.serien_nr, crane.tel_nr, crane.ip,
                crane.rueckmeldung, crane.it_nr,
                crane.kundenkran, crane.lizenz_ja,
                crane.lizenzdatum, crane.bezahlt_bis_rg_erstellt,
                crane.servicemeldung, crane.amount
            ])

        return response

    # 📄 Pagination AFTER export block
    paginator = Paginator(queryset, 10)
    page_obj = paginator.get_page(request.GET.get('page'))

    return render(request, 'data_retrival.html', {
        'page_obj': page_obj
    })

   
def logout_view(request):
    logout(request)
    response = redirect('login')
    # Prevent bfcache (back-forward cache) on logout
    response['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response['Pragma'] = 'no-cache'
    return response

def custom_404(request, exception):
    return render(request, '404.html', status=404)

handler404 = 'project_name.urls.custom_404'

@login_required
def clear_entry(request, pk):
    kran = get_object_or_404(Crane, pk=pk)

    if request.method == "POST" and kran.lizenzdatum:
        _mark_due_paid_in_background(kran)

    return redirect('analyst_dashboard')

@login_required
def toggle_status(request, pk):
    """Toggle crane active/inactive status via AJAX"""
    if request.method == "POST":
        kran = get_object_or_404(Crane, pk=pk)
        kran.is_active = not kran.is_active
        kran.save()
        
        from django.http import JsonResponse
        return JsonResponse({'success': True, 'is_active': kran.is_active})
    
    return JsonResponse({'success': False, 'error': 'Invalid request'}, status=400)

@login_required(login_url='login')
@never_cache
def search_rg(request):
    """Search cranes by computed due years between Lizenzdatum and Bezahlt bis Rg.erstellt."""
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

            _, next_due, _ = _mark_due_paid_in_background(crane)
            if not next_due:
                messages.info(
                    request,
                    f'ID {crane.id}: no pending dues left (already at expiry).'
                )
                return redirect(redirect_url)

            if next_due == expiry_date:
                messages.success(
                    request,
                    f'ID {crane.id}: payment marked. Contract is fully paid until expiry ({crane.bezahlt_bis_rg_erstellt}).'
                )
            else:
                messages.success(
                    request,
                    f'ID {crane.id}: payment marked. Next due moved to {next_due.strftime("%Y-%m-%d")}.'
                )
            return redirect(redirect_url)

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
                messages.info(
                    request,
                    f'ID {crane.id}: cannot move due backward (already at first due year).'
                )
                return redirect(redirect_url)

            messages.success(
                request,
                f'ID {crane.id}: payment reverted. Due moved back to {previous_due.strftime("%Y-%m-%d")}.'
            )
            return redirect(redirect_url)

    # Date filter params
    year = request.GET.get('year', '').strip()
    month = request.GET.get('month', '').strip()
    day = request.GET.get('day', '').strip()
    
    queryset = list(Crane.objects.filter(is_active=True).select_related('due_status'))

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

    queryset = [
        crane for crane in queryset
        if _matches_due_filter(crane, year, month_num, day_num)
    ]
    
    # Sorting
    # Keep default ordering consistent with data() so IDs appear uniform.
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
    
    # Export CSV
    if request.GET.get('export') == 'true':
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="crane_rg_search.csv"'
        
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
    
    # Pagination
    paginator = Paginator(queryset, 10)
    page_obj = paginator.get_page(request.GET.get('page'))
    
    # Get unique years and months for dropdowns
    active_cranes = Crane.objects.filter(is_active=True).select_related('due_status')
    years = sorted(
        {
            str(due_year)
            for crane in active_cranes
            for due_year in _due_years_for_crane(crane)
        },
        reverse=True
    )
    
    months = list(range(1, 13))
    days = list(range(1, 32))
    
    return render(request, 'search_retrival.html', {
        'page_obj': page_obj,
        'years': years,
        'months': months,
        'days': days,
        'selected_year': year,
        'selected_month': month,
        'selected_day': day,
        'sort_by': sort_by,
        'order': order
    })


@login_required(login_url='login')
@never_cache
def update_rg(request):
    """Update expiry date or mark current due year paid for selected crane rows."""
    query = request.GET.get('q', '').strip()
    queryset = Crane.objects.all().order_by('id')

    if query:
        queryset = queryset.filter(serien_nr__icontains=query)

    if request.method == 'POST':
        action = request.POST.get('action', 'update_expiry').strip()
        crane_id = request.POST.get('crane_id', '').strip()
        new_date = request.POST.get('bezahlt_bis_rg_erstellt', '').strip()
        redirect_url = request.POST.get('next') or '/update_rg/'

        if not crane_id.isdigit():
            messages.error(request, 'Invalid row selected.')
            return redirect(redirect_url)

        crane = get_object_or_404(Crane, pk=int(crane_id))

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
                messages.info(
                    request,
                    f'ID {crane.id}: no pending dues left (already at expiry).'
                )
                return redirect(redirect_url)

            _, next_due, _ = _mark_due_paid_in_background(crane)
            if not next_due:
                messages.info(
                    request,
                    f'ID {crane.id}: no pending dues left (already at expiry).'
                )
                return redirect(redirect_url)

            if next_due == expiry_date:
                messages.success(
                    request,
                    f'ID {crane.id}: payment marked. Contract is fully paid until expiry ({crane.bezahlt_bis_rg_erstellt}).'
                )
            else:
                messages.success(
                    request,
                    f'ID {crane.id}: payment marked. Next due moved to {next_due.strftime("%Y-%m-%d")}.'
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

        messages.success(
            request,
            f'Updated Bezahlt bis Rg.erstellt for ID {crane.id}.'
        )
        return redirect(redirect_url)

    paginator = Paginator(queryset, 10)
    page_obj = paginator.get_page(request.GET.get('page'))

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

        Crane.objects.create(
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

        messages.success(request, 'New crane entry created successfully.')
        return redirect('create_rg')

    return render(request, 'create.html', {'form_data': {}})