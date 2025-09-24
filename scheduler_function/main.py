import os
import requests
from datetime import datetime, timedelta, timezone
import google.auth.transport.requests
import google.oauth2.id_token

def trigger_ingest_pipeline(request):
    API_URL = os.getenv('INGEST_API_URL')
    if not API_URL:
        return "Erro: A variável de ambiente INGEST_API_URL não está configurada.", 500

    tz_sp = timezone(timedelta(hours=-3))
    yesterday = datetime.now(tz_sp) - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')

    payload = {
        "start_date": yesterday_str,
        "end_date": yesterday_str
    }

    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, API_URL)

    headers = {
        "Authorization": f"Bearer {id_token}",
        "Content-Type": "application/json"
    }
    response = requests.post(API_URL, json=payload, headers=headers)

    if response.status_code == 200:
        print(f"API chamada com sucesso: {response.text}")
        return "OK", 200
    else:
        print(f"Erro ao chamar a API: {response.status_code} - {response.text}")
        return f"Erro: {response.text}", response.status_code