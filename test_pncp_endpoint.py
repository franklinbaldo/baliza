import requests
import datetime
import json
import pytest

# Configuration
BASE_URL = "https://pncp.gov.br/api/consulta"
ENDPOINT_PATH = "/v1/contratacoes/publicacao" # Testing the 'contratacoes' endpoint

@pytest.mark.parametrize("test_date_str", [(datetime.date.today() - datetime.timedelta(days=1)).isoformat()])
def test_pncp_api_endpoint(test_date_str):
    """
    Tests connectivity and basic response from the PNCP API for a given date.
    """
    print(f"Attempting to connect to PNCP API endpoint: {ENDPOINT_PATH} for date: {test_date_str}\n")

    params = {
        "dataInicial": test_date_str.replace("-", ""),
        "dataFinal": test_date_str.replace("-", ""),
        "pagina": 1,
        "tamanhoPagina": 10, # Requesting only a few items for a quick test
        "codigoModalidadeContratacao": 1 # Added as a guess
    }

    full_url = f"{BASE_URL}{ENDPOINT_PATH}"

    headers = {
        "User-Agent": "baliza-test/0.1.0"
    }

    try:
        print(f"Making GET request to: {full_url}")
        print(f"With parameters: {params}\n")

        response = requests.get(full_url, params=params, headers=headers, timeout=30) # 30-second timeout

        print(f"HTTP Status Code: {response.status_code}\n")

        assert response.status_code in [200, 204], f"Expected status code 200 or 204, got {response.status_code}. Response: {response.text}"
        if response.status_code == 204:
            print("Received 204 No Content. This is expected if no data for the given date/parameters.")
            return # Exit the test gracefully if 204 is received

        print("Successfully received a response from the server.")
        try:
            data = response.json()
            print("\nResponse JSON (partial snippet):")
            # Print a small part of the data to give an idea of the structure
            # For example, totalPaginas and number of items on the current page
            total_paginas = data.get("totalPaginas")
            items = data.get("items", [])
            num_items = len(items)

            print(f"  Total Pages (totalPaginas): {total_paginas}")
            print(f"  Items on this page (count): {num_items}")

            if num_items > 0:
                print(f"\n  First item on this page (first 200 chars):")
                print(json.dumps(items[0], indent=2, ensure_ascii=False)[:200] + "...")
            else:
                print("  No items found on the first page for this date.")

            print("\nTest Insight: If 'Total Pages' and 'Items on this page' are numbers (or 0 if no data for the date), the API is responding correctly.")

        except requests.exceptions.JSONDecodeError:
            print("Failed to decode JSON response from the server.")
            print("Response text (first 500 chars):")
            print(response.text[:500])
            pytest.fail("Failed to decode JSON response")

    except requests.exceptions.Timeout:
        print("The request timed out. The server might be slow or unreachable.")
        pytest.fail("Request timed out")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the request: {e}")
        pytest.fail(f"Request exception: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        pytest.fail(f"Unexpected error: {e}")

