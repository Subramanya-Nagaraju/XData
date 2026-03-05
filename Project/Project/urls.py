
from django.contrib import admin
from django.urls import path
from app import views as app_views
from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    path('', app_views.login, name='login'),
    path('index/', app_views.index, name='index'),
    path('analyst_dashboard/', app_views.data, name='analyst_dashboard'),
    path('search_rg/', app_views.search_rg, name='search_rg'),
    path('search_paid/', app_views.search_paid, name='search_paid'),
    path('update_rg/', app_views.update_rg, name='update_rg'),
    path('create_rg/', app_views.create_rg, name='create_rg'),
    path('toggle_status/<int:pk>/', app_views.toggle_status, name='toggle_status'),
    path('admin/', admin.site.urls),
    path('logout/', app_views.logout_view, name='logout'),
    path('404/', app_views.custom_404, name='custom_404'),
    path('clear/<int:pk>/', app_views.clear_entry, name='clear_entry'),
]

handler404 = 'app.views.custom_404'

# Serve static files in DEBUG mode
if settings.DEBUG:
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
