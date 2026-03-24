import csv
import os
from datetime import date
from django.core.management.base import BaseCommand
from django.conf import settings
from app.models import Crane, Termination, ChangeHistory, CraneDueTracking


def _parse_iso_date(value):
    """Parse ISO date from various formats."""
    if not value:
        return None
    if isinstance(value, date):
        return value
    value_str = str(value).strip()
    if not value_str:
        return None
    date_part = value_str.replace("T", " ").split()[0]
    normalized = date_part.replace("/", "-").replace(".", "-")
    from datetime import datetime
    for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(normalized, fmt).date()
        except ValueError:
            continue
    return None


def _current_due_date_for_crane(crane):
    """Get computed current due date."""
    initial_due_date = _parse_iso_date(crane.bezahlt_bis_rg_erstellt)
    if not initial_due_date:
        return None
    due_status = getattr(crane, 'due_status', None)
    if due_status:
        tracked_due_str = due_status.next_due_date
        if tracked_due_str:
            tracked_due = _parse_iso_date(tracked_due_str)
            if tracked_due and tracked_due > initial_due_date:
                return tracked_due
    return initial_due_date


def _due_display_date_for_crane(crane):
    """Get display due date as string."""
    current_due = _current_due_date_for_crane(crane)
    if current_due:
        return current_due.strftime('%Y-%m-%d')
    return crane.bezahlt_bis_rg_erstellt


def _last_paid_year_for_crane(crane):
    """Get last paid year from payment history."""
    due_status = getattr(crane, 'due_status', None)
    if not due_status:
        return None
    
    # Use payment history if available
    latest_payment = due_status.payment_history.order_by('-recorded_at', '-id').first()
    if latest_payment and latest_payment.paid_for_due_date:
        paid_date = _parse_iso_date(latest_payment.paid_for_due_date)
        if paid_date:
            return paid_date.year
    
    # Fallback: one year before current due
    current_due = _current_due_date_for_crane(crane)
    if current_due:
        return current_due.year - 1
    
    return None


class Command(BaseCommand):
    help = 'Export all crane data to CSV files in project directory'

    CSV_FILENAMES = [
        'view_data.csv',
        'search_due.csv',
        'search_paid.csv',
        'terminations.csv',
        'history.csv',
    ]

    def handle(self, *args, **options):
        # Get data directory (Project/data)
        project_dir = str(settings.BASE_DIR)
        data_dir = os.path.join(project_dir, 'data')
        
        # Create data directory if it doesn't exist
        os.makedirs(data_dir, exist_ok=True)

        # Remove old duplicate CSVs from previous export locations.
        self._cleanup_legacy_csv_files(project_dir)
        
        self.export_view_data(data_dir)
        self.export_search_due(data_dir)
        self.export_search_paid(data_dir)
        self.export_terminations(data_dir)
        self.export_history(data_dir)
        
        self.stdout.write(self.style.SUCCESS(f'✓ All CSV files exported to {data_dir}'))

    def _cleanup_legacy_csv_files(self, project_dir):
        legacy_dirs = [
            project_dir,
            os.path.join(project_dir, 'app'),
        ]

        data_dir = os.path.join(project_dir, 'data')

        for legacy_dir in legacy_dirs:
            if os.path.abspath(legacy_dir) == os.path.abspath(data_dir):
                continue

            for filename in self.CSV_FILENAMES:
                legacy_path = os.path.join(legacy_dir, filename)
                if os.path.exists(legacy_path):
                    try:
                        os.remove(legacy_path)
                        self.stdout.write(f'  - Removed legacy file: {legacy_path}')
                    except OSError:
                        self.stdout.write(f'  - Could not remove legacy file: {legacy_path}')

    def export_view_data(self, data_dir):
        """Export View Data page (all cranes with filters)."""
        filepath = os.path.join(data_dir, 'view_data.csv')
        
        queryset = Crane.objects.select_related('due_status').all().order_by('id')
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([
                'ID', 'Kran Typ', 'Fabrik Nr', 'Kunde', 'LG', 'Kundenummer',
                'Version', 'Serien Nr', 'Tel Nr', 'IP', 'Rueckmeldung', 'IT Nr',
                'Kundenkran', 'Lizenz Ja', 'Lizenzdatum', 'Bezahlt bis Rg.erstellt',
                'Last Paid Year', 'Servicemeldung', 'Amount', 'Status'
            ])
            
            for crane in queryset:
                writer.writerow([
                    crane.id, crane.kran_typ, crane.fabrik_nr, crane.kunde,
                    crane.lg, crane.kundenummer, crane.version, crane.serien_nr,
                    crane.tel_nr, crane.ip, crane.rueckmeldung,
                    crane.it_nr, crane.kundenkran, crane.lizenz_ja,
                    crane.lizenzdatum, _due_display_date_for_crane(crane),
                    _last_paid_year_for_crane(crane),
                    crane.servicemeldung,
                    crane.amount,
                    'Active' if crane.is_active else 'Inactive'
                ])
        
        self.stdout.write(f'  ✓ Exported {queryset.count()} records to view_data.csv')

    def export_search_due(self, data_dir):
        """Export Search Due page."""
        filepath = os.path.join(data_dir, 'search_due.csv')
        
        # Get all cranes
        queryset = Crane.objects.select_related('due_status').all().order_by('id')
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([
                'ID', 'Kran Typ', 'Fabrik Nr', 'Kunde', 'LG', 'Kundenummer',
                'Version', 'Serien Nr', 'Tel Nr', 'IP', 'Rueckmeldung', 'IT Nr',
                'Kundenkran', 'Lizenz Ja', 'Lizenzdatum', 'Bezahlt bis Rg.erstellt',
                'Last Paid Year', 'Servicemeldung', 'Amount', 'Status'
            ])
            
            for crane in queryset:
                writer.writerow([
                    crane.id, crane.kran_typ, crane.fabrik_nr, crane.kunde,
                    crane.lg, crane.kundenummer, crane.version, crane.serien_nr,
                    crane.tel_nr, crane.ip, crane.rueckmeldung,
                    crane.it_nr, crane.kundenkran, crane.lizenz_ja,
                    crane.lizenzdatum, _due_display_date_for_crane(crane),
                    _last_paid_year_for_crane(crane),
                    crane.servicemeldung,
                    crane.amount,
                    'Active' if crane.is_active else 'Inactive'
                ])
        
        self.stdout.write(f'  ✓ Exported {queryset.count()} records to search_due.csv')

    def export_search_paid(self, data_dir):
        """Export Search Paid page."""
        filepath = os.path.join(data_dir, 'search_paid.csv')
        
        # Get all cranes with payment history
        queryset = Crane.objects.select_related('due_status').all().order_by('id')
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([
                'ID', 'Kran Typ', 'Fabrik Nr', 'Kunde', 'LG', 'Kundenummer',
                'Version', 'Serien Nr', 'Tel Nr', 'IP', 'Rueckmeldung', 'IT Nr',
                'Kundenkran', 'Lizenz Ja', 'Lizenzdatum', 'Bezahlt bis Rg.erstellt',
                'Last Paid Year', 'Servicemeldung', 'Amount', 'Status',
                'Payment Date (Actual)', 'Recorded At (System)'
            ])
            
            for crane in queryset:
                due = getattr(crane, 'due_status', None)
                actual_paid = str(due.actual_paid_date) if due and due.actual_paid_date else ''
                recorded_at = due.last_paid_at.strftime('%Y-%m-%d %H:%M:%S') if due and due.last_paid_at else ''
                
                writer.writerow([
                    crane.id, crane.kran_typ, crane.fabrik_nr, crane.kunde,
                    crane.lg, crane.kundenummer, crane.version, crane.serien_nr,
                    crane.tel_nr, crane.ip, crane.rueckmeldung,
                    crane.it_nr, crane.kundenkran, crane.lizenz_ja,
                    crane.lizenzdatum, _due_display_date_for_crane(crane),
                    _last_paid_year_for_crane(crane),
                    crane.servicemeldung,
                    crane.amount,
                    'Active' if crane.is_active else 'Inactive',
                    actual_paid,
                    recorded_at,
                ])
        
        self.stdout.write(f'  ✓ Exported {queryset.count()} records to search_paid.csv')

    def export_terminations(self, data_dir):
        """Export Terminations page."""
        filepath = os.path.join(data_dir, 'terminations.csv')
        
        queryset = Termination.objects.select_related('crane', 'terminated_by').order_by('-terminated_at')
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
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
        
        self.stdout.write(f'  ✓ Exported {queryset.count()} records to terminations.csv')

    def export_history(self, data_dir):
        """Export History/Change Log page."""
        filepath = os.path.join(data_dir, 'history.csv')
        
        queryset = ChangeHistory.objects.select_related('changed_by', 'crane').all().order_by('-changed_at')
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
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
        
        self.stdout.write(f'  ✓ Exported {queryset.count()} records to history.csv')
