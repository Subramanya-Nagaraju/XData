from django.contrib import admin
from .models import ChangeHistory, CranePaymentHistory, Termination


@admin.register(CranePaymentHistory)
class CranePaymentHistoryAdmin(admin.ModelAdmin):
    list_display = ('id', 'crane_id', 'crane_kunde', 'paid_for_due_date', 'actual_paid_date', 'recorded_at')
    list_filter = ('actual_paid_date', 'recorded_at')
    search_fields = ('due_tracking__crane__kunde', 'due_tracking__crane__serien_nr')
    readonly_fields = ('recorded_at',)
    ordering = ('-recorded_at', '-id')

    @admin.display(description='Crane ID')
    def crane_id(self, obj):
        return obj.due_tracking.crane_id

    @admin.display(description='Kunde')
    def crane_kunde(self, obj):
        return obj.due_tracking.crane.kunde


@admin.register(ChangeHistory)
class ChangeHistoryAdmin(admin.ModelAdmin):
    list_display = ('id', 'changed_at', 'action', 'crane_display_id', 'crane_kunde', 'changed_by')
    list_filter = ('action', 'changed_at', 'changed_by')
    search_fields = ('crane_display_id', 'crane_kunde', 'details', 'changed_by__username')
    readonly_fields = ('changed_at',)
    ordering = ('-changed_at', '-id')


@admin.register(Termination)
class TerminationAdmin(admin.ModelAdmin):
    list_display = ('id', 'crane_id', 'crane_kunde', 'original_lizenzdatum', 'original_expiry_date', 'terminated_at', 'terminated_by', 'termination_reason')
    list_filter = ('terminated_at', 'terminated_by')
    search_fields = ('crane__kunde', 'crane__serien_nr', 'termination_reason')
    readonly_fields = ('terminated_at',)
    ordering = ('-terminated_at',)

    @admin.display(description='Kunde')
    def crane_kunde(self, obj):
        return obj.crane.kunde
