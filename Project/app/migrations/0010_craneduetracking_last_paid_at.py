from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('app', '0009_termination'),
    ]

    operations = [
        migrations.AddField(
            model_name='craneduetracking',
            name='last_paid_at',
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
