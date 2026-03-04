from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('app', '0004_crane_is_active'),
    ]

    operations = [
        migrations.RenameField(
            model_name='crane',
            old_name='ip_rueckmeldung',
            new_name='ip',
        ),
        migrations.RenameField(
            model_name='crane',
            old_name='it_nr',
            new_name='rueckmeldung',
        ),
        migrations.RenameField(
            model_name='crane',
            old_name='kundenkran_miete',
            new_name='it_nr',
        ),
        migrations.RenameField(
            model_name='crane',
            old_name='lizenz_janein',
            new_name='kundenkran',
        ),
        migrations.RenameField(
            model_name='crane',
            old_name='lizenzdatum',
            new_name='lizenz_ja',
        ),
        migrations.RenameField(
            model_name='crane',
            old_name='bezahlt_bis',
            new_name='lizenzdatum',
        ),
        migrations.RenameField(
            model_name='crane',
            old_name='rg_erstellt',
            new_name='bezahlt_bis_rg_erstellt',
        ),
    ]
