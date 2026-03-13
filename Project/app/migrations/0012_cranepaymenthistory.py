from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('app', '0011_craneduetracking_actual_paid_date'),
    ]

    operations = [
        migrations.CreateModel(
            name='CranePaymentHistory',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('paid_for_due_date', models.DateField()),
                ('actual_paid_date', models.DateField()),
                ('recorded_at', models.DateTimeField(auto_now_add=True)),
                ('due_tracking', models.ForeignKey(on_delete=models.deletion.CASCADE, related_name='payment_history', to='app.craneduetracking')),
            ],
            options={
                'ordering': ('-recorded_at', '-id'),
            },
        ),
    ]
