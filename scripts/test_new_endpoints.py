#!/usr/bin/env python3
"""
Test script for new PNCP endpoints in BALIZA Phase 3
Tests the new endpoints: /v1/pca/usuario, /v1/pca/, /v1/contratacoes/proposta
"""

import requests
import datetime
import json

BASE_URL = "https://pncp.gov.br/api/consulta"

def test_pca_usuario():
    """Test /v1/pca/usuario endpoint"""
    print("\n--- Testing /v1/pca/usuario endpoint ---")
    
    endpoint_path = "/v1/pca/usuario"
    params = {
        "anoPca": 2024,
        "idUsuario": 1,  # This would need to be a valid user ID
        "pagina": 1,
        "tamanhoPagina": 10
    }
    
    full_url = f"{BASE_URL}{endpoint_path}"
    headers = {"User-Agent": "baliza-api-tester/0.2.0"}
    
    try:
        print(f"Making GET request to: {full_url}")
        print(f"With parameters: {params}")
        
        response = requests.get(full_url, params=params, headers=headers, timeout=30)
        print(f"HTTP Status Code: {response.status_code}")
        
        if response.status_code == 204:
            print("No content for the given parameters.")
        elif response.status_code == 200:
            data = response.json()
            print("\n--- API Response (first 500 characters) ---")
            print(json.dumps(data, indent=2, ensure_ascii=False)[:500])
            if len(json.dumps(data, indent=2, ensure_ascii=False)) > 500:
                print("... (response truncated)")
            print("\n--- End API Response ---")
        else:
            print(f"Response content: {response.text}")
            
    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the request: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response content: {e.response.text}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON response: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def test_pca_general():
    """Test /v1/pca/ endpoint (general PCA)"""
    print("\n--- Testing /v1/pca/ endpoint (general) ---")
    
    endpoint_path = "/v1/pca/"  # Note the trailing slash
    params = {
        "anoPca": 2024,
        "codigoClassificacaoSuperior": "01",  # Common classification code
        "pagina": 1,
        "tamanhoPagina": 10
    }
    
    full_url = f"{BASE_URL}{endpoint_path}"
    headers = {"User-Agent": "baliza-api-tester/0.2.0"}
    
    try:
        print(f"Making GET request to: {full_url}")
        print(f"With parameters: {params}")
        
        response = requests.get(full_url, params=params, headers=headers, timeout=30)
        print(f"HTTP Status Code: {response.status_code}")
        
        if response.status_code == 204:
            print("No content for the given parameters.")
        elif response.status_code == 200:
            data = response.json()
            print("\n--- API Response (first 500 characters) ---")
            print(json.dumps(data, indent=2, ensure_ascii=False)[:500])
            if len(json.dumps(data, indent=2, ensure_ascii=False)) > 500:
                print("... (response truncated)")
            print("\n--- End API Response ---")
        else:
            print(f"Response content: {response.text}")
            
    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the request: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response content: {e.response.text}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON response: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def test_contratacoes_proposta():
    """Test /v1/contratacoes/proposta endpoint"""
    print("\n--- Testing /v1/contratacoes/proposta endpoint ---")
    
    endpoint_path = "/v1/contratacoes/proposta"
    today = datetime.date.today()
    
    params = {
        "dataFinal": today.isoformat().replace("-", ""),  # Format as yyyyMMdd
        "pagina": 1,
        "tamanhoPagina": 10
    }
    
    full_url = f"{BASE_URL}{endpoint_path}"
    headers = {"User-Agent": "baliza-api-tester/0.2.0"}
    
    try:
        print(f"Making GET request to: {full_url}")
        print(f"With parameters: {params}")
        
        response = requests.get(full_url, params=params, headers=headers, timeout=30)
        print(f"HTTP Status Code: {response.status_code}")
        
        if response.status_code == 204:
            print("No content for the given parameters.")
        elif response.status_code == 200:
            data = response.json()
            print("\n--- API Response (first 500 characters) ---")
            print(json.dumps(data, indent=2, ensure_ascii=False)[:500])
            if len(json.dumps(data, indent=2, ensure_ascii=False)) > 500:
                print("... (response truncated)")
            print("\n--- End API Response ---")
        else:
            print(f"Response content: {response.text}")
            
    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the request: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response content: {e.response.text}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON response: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def main():
    """Run all tests"""
    print("=== BALIZA Phase 3 - New Endpoints Test ===")
    print("Testing new endpoints added in Phase 3:")
    print("1. /v1/pca/usuario")
    print("2. /v1/pca/")
    print("3. /v1/contratacoes/proposta")
    print("=" * 50)
    
    test_pca_usuario()
    test_pca_general()
    test_contratacoes_proposta()
    
    print("\n=== Test Complete ===")
    print("Note: Some endpoints may return 400/422 errors due to")
    print("hardcoded test parameters. This is expected behavior.")
    print("The main goal is to verify the endpoints are reachable")
    print("and the API structure is correct.")


if __name__ == "__main__":
    main()