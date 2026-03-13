from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('app', '0008_delete_departmentdata'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='Termination',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('terminated_at', models.DateTimeField(auto_now_add=True)),
                ('termination_reason', models.TextField(blank=True, default='')),
                ('original_expiry_date', models.CharField(max_length=20)),
                ('original_lizenzdatum', models.CharField(max_length=20)),
                ('crane', models.ForeignKey(
                    on_delete=django.db.models.deletion.PROTECT,
                    related_name='terminations',
                    to='app.crane',
                )),
                ('terminated_by', models.ForeignKey(
                    blank=True,
                    null=True,
                    on_delete=django.db.models.deletion.SET_NULL,
                    related_name='terminations',
                    to=settings.AUTH_USER_MODEL,
                )),
            ],
        ),
    ]
