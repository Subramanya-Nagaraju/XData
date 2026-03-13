from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('app', '0012_cranepaymenthistory'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='ChangeHistory',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('crane_display_id', models.CharField(blank=True, default='', max_length=50)),
                ('crane_kunde', models.CharField(blank=True, default='', max_length=255)),
                ('action', models.CharField(max_length=80)),
                ('details', models.TextField(blank=True, default='')),
                ('changed_at', models.DateTimeField(auto_now_add=True)),
                ('changed_by', models.ForeignKey(blank=True, null=True, on_delete=models.deletion.SET_NULL, related_name='change_history_entries', to=settings.AUTH_USER_MODEL)),
                ('crane', models.ForeignKey(blank=True, null=True, on_delete=models.deletion.SET_NULL, related_name='change_history_entries', to='app.crane')),
            ],
            options={
                'ordering': ('-changed_at', '-id'),
            },
        ),
    ]
