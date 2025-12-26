import requests
from django.http import HttpResponse

AIRFLOW_BASE_URL = 'http://localhost:8080'

def airflow_proxy(request, path=''):
    # Ensure leading slash
    if not path.startswith('/'):
        path = '/' + path

    target_url = f'{AIRFLOW_BASE_URL}{path}'
    if request.META.get('QUERY_STRING'):
        target_url += '?' + request.META['QUERY_STRING']

    headers = {k: v for k, v in request.headers.items()
               if k.lower() not in ['host', 'content-length', 'connection']}

    try:
        response = requests.request(
            method=request.method,
            url=target_url,
            headers=headers,
            data=request.body,
            allow_redirects=False,
            timeout=30
        )

        django_response = HttpResponse(
            content=response.content,
            status=response.status_code,
            content_type=response.headers.get('content-type', 'application/octet-stream')
        )

        excluded_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
        for header, value in response.headers.items():
            if header.lower() not in excluded_headers:
                django_response[header] = value

        return django_response

    except requests.RequestException as e:
        return HttpResponse(f'Proxy error: {str(e)}', status=502)
