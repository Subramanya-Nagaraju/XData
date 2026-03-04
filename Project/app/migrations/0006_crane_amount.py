from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('app', '0005_rename_crane_fields'),
    ]

    operations = [
        migrations.AddField(
            model_name='crane',
            name='amount',
            field=models.IntegerField(blank=True, null=True),
        ),
    ]
