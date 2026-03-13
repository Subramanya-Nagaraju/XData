from django.urls import NoReverseMatch, reverse


class DisableBackCacheMiddleware:
    """Disable client-side caching so protected pages cannot be reopened after logout."""

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)

        try:
            login_path = reverse('login')
            logout_path = reverse('logout')
            auth_paths = {login_path, logout_path}
        except NoReverseMatch:
            auth_paths = set()

        if request.path in auth_paths or getattr(request.user, 'is_authenticated', False):
            response['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0, private'
            response['Pragma'] = 'no-cache'
            response['Expires'] = '0'

        return response
