import requests
import datetime
import json

BASE_URL = "https://pncp.gov.br/api/consulta"
ENDPOINT_PATH = "/v1/contratacoes/publicacao"

def test_api_call(start_date_str, end_date_str):
    print(f"\n--- Testing PNCP API for {start_date_str} to {end_date_str} ---")

    params = {
        "dataInicial": start_date_str.replace("-", ""),
        "dataFinal": end_date_str.replace("-", ""),
        "pagina": 1,
        "tamanhoPagina": 10, # Requesting a small page size for testing
        "codigoModalidadeContratacao": 1 # Re-adding as it's a required parameter
    }

    full_url = f"{BASE_URL}{ENDPOINT_PATH}"

    headers = {
        "User-Agent": "baliza-api-tester/0.1.0"
    }

    try:
        print(f"Making GET request to: {full_url}")
        print(f"With parameters: {params}")

        response = requests.get(full_url, params=params, headers=headers, timeout=30)
        response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)

        print(f"HTTP Status Code: {response.status_code}")
        
        if response.status_code == 204:
            print("No content for the given date and parameters.")
        else:
            data = response.json()
            print("\n--- API Response (first 500 characters) ---")
            print(json.dumps(data, indent=2, ensure_ascii=False)[:500])
            if len(json.dumps(data, indent=2, ensure_ascii=False)) > 500:
                print("... (response truncated)")
            print("\n--- End API Response ---")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the request: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response content: {e.response.text}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON response: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    # Get a date range (e.g., last 30 days)
    today = datetime.date.today()
    start_date = today - datetime.timedelta(days=30)
    end_date = today - datetime.timedelta(days=1) # Yesterday

    test_api_call(start_date.isoformat(), end_date.isoformat())