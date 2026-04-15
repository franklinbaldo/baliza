import time
from datetime import datetime

import httpx


def debug_probe():
    url = "https://pncp.gov.br/api/consulta/v1/contratos"
    params = {
        "dataInicial": "20260329",
        "dataFinal": "20260329",
        "tamanhoPagina": 500,
        "pagina": 1,
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    }

    print(f"[{datetime.now()}] Starting probe (HTTP/2 + Browser UA)...")
    start = time.time()

    with httpx.Client(http2=True, timeout=180.0) as client:
        try:
            print(f"[{datetime.now()}] Sending GET {url} with params {params}...")
            response = client.get(url, params=params, headers=headers)
            duration = time.time() - start
            print(f"[{datetime.now()}] Response received in {duration:.2f}s")
            print(f"Status: {response.status_code}")
            print(f"HTTP Version: {response.http_version}")
            if response.status_code == 200:
                data = response.json()
                print(f"Total Pages: {data.get('totalPaginas')}")
                print(f"Total Records: {data.get('totalRegistros')}")
            else:
                print(f"Error Body: {response.text}")
        except Exception as e:
            print(f"[{datetime.now()}] Request failed: {type(e).__name__}: {e}")


if __name__ == "__main__":
    debug_probe()
