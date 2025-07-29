#!/usr/bin/env python3
"""
Script to discover available parameter values for PNCP endpoints
"""

import requests
import json
from datetime import datetime, timedelta
from urllib.parse import urljoin

BASE_URL = "https://pncp.gov.br/api/consulta/v1/"


def test_parameter_values(endpoint, base_params, param_name, test_values):
    """Test different parameter values and track successful ones"""

    print(f"\n{'=' * 60}")
    print(f"Testing parameter '{param_name}' for endpoint: {endpoint}")
    print(f"{'=' * 60}")

    working_values = []

    for i, value in enumerate(test_values, 1):
        params = base_params.copy()
        params[param_name] = value

        url = urljoin(BASE_URL, endpoint)
        print(f"\n{i}. Testing {param_name}={value}")

        try:
            response = requests.get(url, params=params, timeout=30)
            print(f"   Status: {response.status_code}")

            if response.status_code == 200:
                try:
                    data = response.json()
                    if isinstance(data, dict) and "data" in data:
                        item_count = len(data.get("data", []))
                        total_records = data.get("totalRegistros", 0)
                        print(
                            f"   ✅ SUCCESS: {item_count} items returned, {total_records} total records"
                        )
                        working_values.append(
                            {
                                "value": value,
                                "item_count": item_count,
                                "total_records": total_records,
                            }
                        )
                    else:
                        print("   ✅ SUCCESS: Valid response structure")
                        working_values.append(
                            {"value": value, "item_count": 0, "total_records": 0}
                        )

                except json.JSONDecodeError:
                    print("   ⚠️  SUCCESS but no JSON")

            elif response.status_code == 204:
                print("   ✅ SUCCESS: No content (valid)")
                working_values.append(
                    {"value": value, "item_count": 0, "total_records": 0}
                )

            else:
                print(f"   ❌ FAILED: {response.text[:100]}...")

        except requests.exceptions.RequestException as e:
            print(f"   ❌ REQUEST FAILED: {e}")

    return working_values


def main():
    current_date = datetime.now()
    thirty_days_ago = (current_date - timedelta(days=30)).strftime("%Y%m%d")
    today = current_date.strftime("%Y%m%d")

    results = {}

    print("=== DISCOVERING PNCP PARAMETER VALUES ===")

    # 1. Test modalidade codes for contratacoes_publicacao
    modalidade_codes = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
    base_params = {
        "tamanhoPagina": 10,
        "pagina": 1,
        "dataInicial": "20210101",
        "dataFinal": "20210131",
    }

    results["modalidades_contratacoes_publicacao"] = test_parameter_values(
        "contratacoes/publicacao",
        base_params,
        "codigoModalidadeContratacao",
        modalidade_codes,
    )

    # 2. Test classification codes for pca
    # Common classification codes from experience
    classification_codes = [
        "00",
        "01",
        "02",
        "03",
        "04",
        "05",
        "10",
        "11",
        "12",
        "20",
        "21",
        "30",
        "40",
        "50",
        "60",
        "70",
        "80",
        "90",
        "99",
    ]
    base_params_pca = {"tamanhoPagina": 10, "pagina": 1, "anoPca": 2024}

    results["classification_codes_pca"] = test_parameter_values(
        "pca/", base_params_pca, "codigoClassificacaoSuperior", classification_codes
    )

    # 3. Test different years for pca
    years = [2021, 2022, 2023, 2024, 2025]
    base_params_pca_year = {
        "tamanhoPagina": 10,
        "pagina": 1,
        "codigoClassificacaoSuperior": "00",
    }

    results["years_pca"] = test_parameter_values(
        "pca/", base_params_pca_year, "anoPca", years
    )

    # 4. Test user IDs for pca_usuario (common IDs from manual)
    user_ids = [1, 2, 3, 4, 5, 36, 100, 194035]  # Mix of small and known IDs
    base_params_usuario = {"tamanhoPagina": 10, "pagina": 1, "anoPca": 2024}

    results["user_ids_pca_usuario"] = test_parameter_values(
        "pca/usuario", base_params_usuario, "idUsuario", user_ids
    )

    # 5. Test modalidade codes for contratacoes_proposta (optional parameter)
    base_params_proposta = {"tamanhoPagina": 10, "pagina": 1, "dataFinal": "20300101"}

    results["modalidades_contratacoes_proposta"] = test_parameter_values(
        "contratacoes/proposta",
        base_params_proposta,
        "codigoModalidadeContratacao",
        modalidade_codes,
    )

    print("\n" + "=" * 80)
    print("SUMMARY OF WORKING PARAMETER VALUES")
    print("=" * 80)

    for category, working_values in results.items():
        print(f"\n{category.upper()}:")
        if working_values:
            for item in working_values:
                print(f"  - {item['value']}: {item['total_records']} records")
        else:
            print("  - No working values found")

    # Generate configuration suggestions
    print("\n" + "=" * 80)
    print("CONFIGURATION SUGGESTIONS")
    print("=" * 80)

    print("\n# Suggested parameter variations for DLT configuration:")

    # Modalidades with data
    modalidades_with_data = [
        item["value"]
        for item in results.get("modalidades_contratacoes_publicacao", [])
        if item["total_records"] > 0
    ]
    if modalidades_with_data:
        print(f"MODALIDADES_WITH_DATA = {modalidades_with_data}")

    # Classification codes with data
    classifications_with_data = [
        item["value"]
        for item in results.get("classification_codes_pca", [])
        if item["total_records"] > 0
    ]
    if classifications_with_data:
        print(f"CLASSIFICATION_CODES_WITH_DATA = {classifications_with_data}")

    # Years with data
    years_with_data = [
        item["value"]
        for item in results.get("years_pca", [])
        if item["total_records"] > 0
    ]
    if years_with_data:
        print(f"YEARS_WITH_DATA = {years_with_data}")

    # User IDs with data
    user_ids_with_data = [
        item["value"]
        for item in results.get("user_ids_pca_usuario", [])
        if item["total_records"] > 0
    ]
    if user_ids_with_data:
        print(f"USER_IDS_WITH_DATA = {user_ids_with_data}")

    return results


if __name__ == "__main__":
    main()
