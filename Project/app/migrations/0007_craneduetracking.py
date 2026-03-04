from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('app', '0006_crane_amount'),
    ]

    operations = [
        migrations.CreateModel(
            name='CraneDueTracking',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('next_due_date', models.CharField(blank=True, max_length=20, null=True)),
                ('crane', models.OneToOneField(on_delete=models.deletion.CASCADE, related_name='due_status', to='app.crane')),
            ],
        ),
    ]
