from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver
from django.core.management import call_command
from .models import Crane, Termination, ChangeHistory, CraneDueTracking, CranePaymentHistory


@receiver(post_save, sender=Crane)
@receiver(post_save, sender=CraneDueTracking)
@receiver(post_save, sender=CranePaymentHistory)
@receiver(post_save, sender=Termination)
@receiver(post_save, sender=ChangeHistory)
@receiver(post_delete, sender=Crane)
@receiver(post_delete, sender=CraneDueTracking)
@receiver(post_delete, sender=CranePaymentHistory)
@receiver(post_delete, sender=Termination)
@receiver(post_delete, sender=ChangeHistory)
def export_csv_on_change(sender, **kwargs):
    """Automatically export CSVs whenever data changes."""
    try:
        call_command('export_csv_data', verbosity=0)
    except Exception as e:
        # Log error but don't break the save operation
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f'CSV export error: {str(e)}')
