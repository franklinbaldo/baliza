#!/usr/bin/env python3
"""
Test script to explore PNCP API endpoints with corrected parameters based on initial findings
"""

import requests
import json
from datetime import datetime, timedelta
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

                except json.JSONDecodeError:
                    print("   Response is not JSON")
                    print(f"   Content length: {len(response.content)}")

            else:
                print(f"   Error: {response.text[:200]}...")

        except requests.exceptions.RequestException as e:
            print(f"   Request failed: {e}")


def main():
    current_date = datetime.now()

    # Based on findings:
    # - contratacoes/proposta requires dataFinal >= current date
    # - instrumentoscobranca requires both dataInicial and dataFinal with max 30 days difference
    # - pca endpoints require anoPca parameter
    # - pca/atualizacao requires dataInicio parameter

    print("=== Testing with corrected parameters ===")

    # 1. Test contratacoes/proposta with future dates (works!)
    proposta_params = [
        {"tamanhoPagina": 50, "pagina": 1, "dataFinal": "20300101"},  # This works
        {
            "tamanhoPagina": 500,
            "pagina": 1,
            "dataFinal": "20300101",
        },  # Try larger page size
    ]

    test_endpoint("contratacoes/proposta", proposta_params)

    # 2. Test instrumentoscobranca with 30-day windows and recent dates
    recent_date_start = (current_date - timedelta(days=30)).strftime("%Y%m%d")
    recent_date_end = current_date.strftime("%Y%m%d")

    # Test with smaller date ranges (max 30 days)
    instrumentos_params = [
        {
            "tamanhoPagina": 50,
            "pagina": 1,
            "dataInicial": recent_date_start,
            "dataFinal": recent_date_end,
        },
        {
            "tamanhoPagina": 50,
            "pagina": 1,
            "dataInicial": "20240701",
            "dataFinal": "20240730",
        },  # 30 days in 2024
        {
            "tamanhoPagina": 50,
            "pagina": 1,
            "dataInicial": "20240601",
            "dataFinal": "20240630",
        },  # 30 days in 2024
    ]

    test_endpoint("instrumentoscobranca/inclusao", instrumentos_params)

    # 3. Test pca endpoints with anoPca parameter
    pca_params = [
        {"tamanhoPagina": 50, "pagina": 1, "anoPca": 2024},
        {"tamanhoPagina": 50, "pagina": 1, "anoPca": 2023},
        {"tamanhoPagina": 50, "pagina": 1, "anoPca": 2021},
    ]

    test_endpoint("pca/", pca_params)

    # 4. Test pca/usuario with anoPca parameter
    pca_usuario_params = [
        {"tamanhoPagina": 50, "pagina": 1, "anoPca": 2024},
        {"tamanhoPagina": 50, "pagina": 1, "anoPca": 2023},
        {"tamanhoPagina": 50, "pagina": 1, "anoPca": 2021},
    ]

    test_endpoint("pca/usuario", pca_usuario_params)

    # 5. Test pca/atualizacao with dataInicio parameter
    pca_atualizacao_params = [
        {
            "tamanhoPagina": 50,
            "pagina": 1,
            "dataInicio": recent_date_start,
            "dataFim": recent_date_end,
        },
        {
            "tamanhoPagina": 50,
            "pagina": 1,
            "dataInicio": "20240701",
            "dataFim": "20240730",
        },
        {"tamanhoPagina": 50, "pagina": 1, "dataInicio": "20240601"},  # Only start date
    ]

    test_endpoint("pca/atualizacao", pca_atualizacao_params)


if __name__ == "__main__":
    main()
