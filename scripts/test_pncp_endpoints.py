#!/usr/bin/env python3
"""
Test script to explore PNCP API endpoints and find working parameters
"""

import requests
import json
from datetime import datetime
from urllib.parse import urljoin

BASE_URL = "https://pncp.gov.br/api/consulta/v1/"


def test_endpoint(endpoint, params_list):
    """Test an endpoint with different parameter combinations"""
    print(f"\n{'=' * 60}")
    print(f"Testing endpoint: {endpoint}")
    print(f"{'=' * 60}")

    for i, params in enumerate(params_list, 1):
        url = urljoin(BASE_URL, endpoint)
        print(f"\n{i}. Testing with params: {params}")
        print(f"   URL: {url}?{'&'.join([f'{k}={v}' for k, v in params.items()])}")

        try:
            response = requests.get(url, params=params, timeout=30)
            print(f"   Status: {response.status_code}")

            if response.status_code == 200:
                try:
                    data = response.json()
                    if isinstance(data, dict):
                        print(f"   Response type: dict with {len(data)} keys")
                        if "data" in data:
                            print(f"   Data items: {len(data.get('data', []))}")
                        print(f"   Keys: {list(data.keys())}")
                    elif isinstance(data, list):
                        print(f"   Response type: list with {len(data)} items")
                    else:
                        print(f"   Response type: {type(data)}")

                    # Show first item structure if available
                    if isinstance(data, dict) and "data" in data and data["data"]:
                        first_item = data["data"][0]
                        print(
                            f"   First item keys: {list(first_item.keys()) if isinstance(first_item, dict) else 'Not a dict'}"
                        )
                    elif isinstance(data, list) and data:
                        first_item = data[0]
                        print(
                            f"   First item keys: {list(first_item.keys()) if isinstance(first_item, dict) else 'Not a dict'}"
                        )

                except json.JSONDecodeError:
                    print("   Response is not JSON")
                    print(f"   Content length: {len(response.content)}")

            else:
                print(f"   Error: {response.text[:200]}...")

        except requests.exceptions.RequestException as e:
            print(f"   Request failed: {e}")


def main():
    # Test different date formats and ranges
    current_date = datetime.now()
    old_date = datetime(2021, 1, 1)
    recent_date = datetime(2024, 1, 1)
    future_date = datetime(2030, 1, 1)

    # Date formats to try
    date_formats = [
        old_date.strftime("%Y%m%d"),  # 20210101
        old_date.strftime("%Y-%m-%d"),  # 2021-01-01
        recent_date.strftime("%Y%m%d"),  # 20240101
        future_date.strftime("%Y%m%d"),  # 20300101
    ]

    # 1. Test contratacoes/proposta endpoint
    proposta_params = [
        {"tamanhoPagina": 50, "pagina": 1, "dataFinal": "20300101"},
        {"tamanhoPagina": 50, "pagina": 1, "dataFinal": "20241231"},
        {
            "tamanhoPagina": 50,
            "pagina": 1,
            "dataInicial": "20210101",
            "dataFinal": "20210131",
        },
        {
            "tamanhoPagina": 50,
            "pagina": 1,
            "dataInicial": "20240101",
            "dataFinal": "20241231",
        },
        {"tamanhoPagina": 50, "pagina": 1},  # No date filter
        {"tamanhoPagina": 10, "pagina": 1, "dataFinal": "20300101"},
    ]

    test_endpoint("contratacoes/proposta", proposta_params)

    # 2. Test instrumentoscobranca/inclusao endpoint
    instrumentos_params = [
        {
            "tamanhoPagina": 50,
            "pagina": 1,
            "dataInicial": "20210101",
            "dataFinal": "20210131",
        },
        {
            "tamanhoPagina": 50,
            "pagina": 1,
            "dataInicial": "20240101",
            "dataFinal": "20241231",
        },
        {"tamanhoPagina": 50, "pagina": 1, "dataFinal": "20300101"},
        {"tamanhoPagina": 50, "pagina": 1},  # No date filter
        {"tamanhoPagina": 10, "pagina": 1, "dataFinal": "20300101"},
    ]

    test_endpoint("instrumentoscobranca/inclusao", instrumentos_params)

    # 3. Test pca endpoint
    pca_params = [
        {"tamanhoPagina": 50, "pagina": 1},
        {"tamanhoPagina": 10, "pagina": 1},
        {
            "tamanhoPagina": 50,
            "pagina": 1,
            "dataInicial": "20210101",
            "dataFinal": "20210131",
        },
        {"tamanhoPagina": 50, "pagina": 1, "dataFinal": "20300101"},
    ]

    test_endpoint("pca/", pca_params)

    # 4. Test pca/usuario endpoint
    pca_usuario_params = [
        {"tamanhoPagina": 50, "pagina": 1},
        {"tamanhoPagina": 10, "pagina": 1},
        {
            "tamanhoPagina": 50,
            "pagina": 1,
            "dataInicial": "20210101",
            "dataFinal": "20210131",
        },
        {"tamanhoPagina": 50, "pagina": 1, "dataFinal": "20300101"},
    ]

    test_endpoint("pca/usuario", pca_usuario_params)

    # 5. Test pca/atualizacao endpoint
    pca_atualizacao_params = [
        {"tamanhoPagina": 50, "pagina": 1},
        {"tamanhoPagina": 10, "pagina": 1},
        {
            "tamanhoPagina": 50,
            "pagina": 1,
            "dataInicial": "20210101",
            "dataFinal": "20210131",
        },
        {"tamanhoPagina": 50, "pagina": 1, "dataFinal": "20300101"},
    ]

    test_endpoint("pca/atualizacao", pca_atualizacao_params)


if __name__ == "__main__":
    main()
