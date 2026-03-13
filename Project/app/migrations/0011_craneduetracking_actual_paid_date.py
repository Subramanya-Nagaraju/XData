from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('app', '0010_craneduetracking_last_paid_at'),
    ]

    operations = [
        migrations.AddField(
            model_name='craneduetracking',
            name='actual_paid_date',
            field=models.DateField(blank=True, null=True),
        ),
    ]
