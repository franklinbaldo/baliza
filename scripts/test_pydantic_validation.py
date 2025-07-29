#!/usr/bin/env python3
"""
Test script to validate Pydantic models against real PNCP API responses.
This will help us identify missing fields and fix our schema definitions.
"""

import requests
from datetime import date, timedelta
from baliza.settings import settings
from baliza.models import (
    PaginaRetornoRecuperarCompraPublicacaoDTO,
    PaginaRetornoRecuperarContratoDTO,
    PaginaRetornoAtaRegistroPrecoPeriodoDTO,
)
from pydantic import ValidationError


def test_endpoint_pydantic_model(
    endpoint_path: str, params: dict, model_class, endpoint_name: str
):
    """Test if a Pydantic model can validate real API response from an endpoint."""

    print(f"\n🔍 Testing {endpoint_name} ({endpoint_path})")
    print(f"   Params: {params}")

    # Make API request
    try:
        url = f"{settings.pncp_api_base_url}{endpoint_path}"
        response = requests.get(url, params=params, timeout=10)

        if response.status_code == 204:
            print("   ⚪ No data found (204 No Content)")
            return
        elif response.status_code != 200:
            print(f"   ❌ API Error: {response.status_code}")
            if response.text:
                print(f"      Response: {response.text}")
            return

        data = response.json()

        # Try to validate with Pydantic model
        try:
            validated = model_class.model_validate(data)
            print("   ✅ Pydantic model validation: SUCCESS")
            print(f"   📊 Records found: {len(data.get('data', []))}")

        except ValidationError as e:
            print("   ❌ Pydantic model validation: FAILED")
            print("   📋 Detailed validation errors:")

            # Analyze specific field values causing errors
            problematic_fields = {}
            for error in e.errors():
                field_path = " -> ".join(str(x) for x in error["loc"])
                if field_path not in problematic_fields:
                    problematic_fields[field_path] = []
                problematic_fields[field_path].append(error["msg"])

            for field_path, errors in problematic_fields.items():
                print(f"      ❌ {field_path}:")
                for error_msg in set(errors):  # Remove duplicates
                    print(f"         - {error_msg}")

                # Show actual values for this field
                if data.get("data") and len(data["data"]) > 0:
                    field_parts = field_path.split(" -> ")
                    if len(field_parts) >= 3 and field_parts[0] == "data":
                        try:
                            record_index = int(field_parts[1])
                            field_name = field_parts[2]
                            if record_index < len(data["data"]):
                                actual_value = data["data"][record_index].get(
                                    field_name
                                )
                                print(
                                    f"         📍 Actual value: {repr(actual_value)} (type: {type(actual_value).__name__})"
                                )

                                # For enum errors, show all unique values found
                                if (
                                    "Input should be" in errors[0]
                                    and "enum" in errors[0].lower()
                                ):
                                    unique_values = set()
                                    for record in data["data"][
                                        :20
                                    ]:  # Check first 20 records
                                        if field_name in record:
                                            unique_values.add(repr(record[field_name]))
                                    print(
                                        f"         📋 All values found: {', '.join(sorted(unique_values))}"
                                    )
                        except (ValueError, IndexError, KeyError):
                            pass

            # Show sample of actual data structure
            if data.get("data") and len(data["data"]) > 0:
                sample_record = data["data"][0]
                print("   📄 Sample API response structure (first record):")
                for field in sorted(sample_record.keys())[:15]:  # Show first 15 fields
                    value = sample_record[field]
                    if value is None:
                        print(f"      - {field}: null")
                    elif isinstance(value, (dict, list)):
                        print(
                            f"      - {field}: {type(value).__name__} ({len(value)} items)"
                            if hasattr(value, "__len__")
                            else f"      - {field}: {type(value).__name__}"
                        )
                    else:
                        print(
                            f"      - {field}: {repr(value)} ({type(value).__name__})"
                        )
                if len(sample_record.keys()) > 15:
                    print(f"      ... and {len(sample_record.keys()) - 15} more fields")

    except Exception as e:
        print(f"   💥 Request failed: {e}")


def main():
    """Test all endpoints with their corresponding Pydantic models."""

    print("🧪 Testing Pydantic Model Validation Against Real PNCP API Responses")
    print("=" * 70)

    # Test date range (use recent dates that are likely to have data)
    recent_date = date.today() - timedelta(days=30)  # 30 days ago
    start_date = recent_date.strftime("%Y%m%d")
    end_date = recent_date.strftime("%Y%m%d")

    # Test cases: (endpoint_path, params, model_class, name)
    # Use parameters that work based on our successful extractions
    test_cases = [
        (
            "/v1/contratos",
            {
                "dataInicial": start_date,
                "dataFinal": end_date,
                "tamanhoPagina": 10,
                "pagina": 1,
            },
            PaginaRetornoRecuperarContratoDTO,
            "contratos",
        ),
        (
            "/v1/contratacoes/publicacao",
            {
                "dataInicial": start_date,
                "dataFinal": end_date,
                "codigoModalidadeContratacao": 8,
                "tamanhoPagina": 10,
                "pagina": 1,
            },
            PaginaRetornoRecuperarCompraPublicacaoDTO,
            "contratacoes_publicacao",
        ),
        (
            "/v1/atas",
            {
                "dataInicial": start_date,
                "dataFinal": end_date,
                "tamanhoPagina": 10,
                "pagina": 1,
            },
            PaginaRetornoAtaRegistroPrecoPeriodoDTO,
            "atas",
        ),
    ]

    for endpoint_path, params, model_class, name in test_cases:
        test_endpoint_pydantic_model(endpoint_path, params, model_class, name)

    print("\n" + "=" * 70)
    print("🎯 Recommendations:")
    print("1. ✅ For endpoints that validate successfully: Keep using Pydantic models")
    print("2. ❌ For endpoints that fail validation: Use permissive column definitions")
    print("3. 🔧 Consider updating Pydantic models to include missing fields")


if __name__ == "__main__":
    main()
