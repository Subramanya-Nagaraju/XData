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

    def __str__(self):
        return f"Due tracking for Crane {self.crane_id}"
    
from django.db import models

class DepartmentData(models.Model):
    department = models.CharField(max_length=50)
    title = models.CharField(max_length=100)
    description = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.department} - {self.title}"