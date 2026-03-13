from django.db import models

class Crane(models.Model):
    kran_typ = models.CharField(max_length=50)
    fabrik_nr = models.CharField(max_length=50)
    kunde = models.CharField(max_length=255)

    lg = models.CharField(max_length=50, null=True, blank=True)
    kundenummer = models.CharField(max_length=50, null=True, blank=True)

    version = models.CharField(max_length=20)
    serien_nr = models.CharField(max_length=50)
    tel_nr = models.CharField(max_length=50)

    ip = models.CharField(max_length=100)

    rueckmeldung = models.CharField(max_length=20)

    it_nr = models.CharField(max_length=50)
    kundenkran = models.CharField(max_length=10)

    lizenz_ja = models.CharField(max_length=20)
    lizenzdatum = models.CharField(max_length=20)
    bezahlt_bis_rg_erstellt = models.CharField(max_length=20)

    amount = models.IntegerField(null=True, blank=True)
    servicemeldung = models.IntegerField()
    is_active = models.BooleanField(default=True)

    def __str__(self):
        return f"{self.kran_typ} - {self.fabrik_nr}"    


class CraneDueTracking(models.Model):
    crane = models.OneToOneField(Crane, on_delete=models.CASCADE, related_name='due_status')
    next_due_date = models.CharField(max_length=20, null=True, blank=True)
    last_paid_at = models.DateTimeField(null=True, blank=True)   # system timestamp when button was clicked
    actual_paid_date = models.DateField(null=True, blank=True)   # date user says client actually paid

    def __str__(self):
        return f"Due tracking for Crane {self.crane_id}"


class CranePaymentHistory(models.Model):
    due_tracking = models.ForeignKey(CraneDueTracking, on_delete=models.CASCADE, related_name='payment_history')
    paid_for_due_date = models.DateField()
    actual_paid_date = models.DateField()
    recorded_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ('-recorded_at', '-id')

    def __str__(self):
        return f"Payment history for Crane {self.due_tracking.crane_id} on {self.actual_paid_date}"


class ChangeHistory(models.Model):
    crane = models.ForeignKey(
        Crane,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='change_history_entries',
    )
    crane_display_id = models.CharField(max_length=50, blank=True, default='')
    crane_kunde = models.CharField(max_length=255, blank=True, default='')
    action = models.CharField(max_length=80)
    details = models.TextField(blank=True, default='')
    changed_by = models.ForeignKey(
        'auth.User',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name='change_history_entries',
    )
    changed_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ('-changed_at', '-id')

    def __str__(self):
        return f"{self.action} - Crane {self.crane_display_id or '-'}"


class Termination(models.Model):
    crane = models.ForeignKey(Crane, on_delete=models.PROTECT, related_name='terminations')
    terminated_at = models.DateTimeField(auto_now_add=True)
    terminated_by = models.ForeignKey(
        'auth.User', null=True, blank=True, on_delete=models.SET_NULL, related_name='terminations'
    )
    termination_reason = models.TextField(blank=True, default='')
    original_expiry_date = models.CharField(max_length=20)
    original_lizenzdatum = models.CharField(max_length=20)

    def __str__(self):
        return f"Termination of Crane {self.crane_id} at {self.terminated_at}"
    
