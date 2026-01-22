#!/usr/bin/env python3
"""
Debug script to test PNCP API endpoints directly
"""

import json
from datetime import datetime

import requests


def test_pncp_endpoint(endpoint_path, params):
    """Test a PNCP API endpoint with given parameters"""
    base_url = "https://pncp.gov.br/api/consulta"
    url = f"{base_url}{endpoint_path}"

    headers = {
        "Accept": "application/json",
        "User-Agent": "Baliza/2.0.0-dev Debug Script",
    }

    print(f"🔍 Testing: {url}")
    print(f"📋 Parameters: {params}")
    print(f"🕐 Time: {datetime.now()}")
    print("-" * 80)

    try:
        response = requests.get(url, params=params, headers=headers, timeout=30)

        print(f"📊 Status Code: {response.status_code}")
        print(f"📏 Content Length: {len(response.content)}")
        print(f"🏷️  Content Type: {response.headers.get('content-type', 'unknown')}")

        if response.status_code == 204:
            print("✅ 204 No Content - Empty result (normal for no data)")
            return {"status": "empty", "data": []}

        if response.status_code != 200:
            print(f"❌ HTTP Error: {response.status_code}")
            print(f"Response: {response.text[:500]}")
            return {"status": "error", "error": f"HTTP {response.status_code}"}

        # Try to parse JSON
        try:
            data = response.json()
            print("✅ Valid JSON received")

            if isinstance(data, dict):
                if "data" in data:
                    print(f"📈 Data array length: {len(data['data'])}")
                    if data["data"]:
                        print("📝 First record:")
                        print(json.dumps(data["data"][0], indent=2))
                    if "totalPaginas" in data:
                        print(f"📄 Total pages: {data['totalPaginas']}")
                else:
                    print(f"🔑 JSON keys: {list(data.keys())}")

            return {"status": "success", "data": data}

        except json.JSONDecodeError as e:
            print(f"❌ JSON Parse Error: {e}")
            print(f"Raw content (first 200 chars): {response.text[:200]}")
            return {
                "status": "json_error",
                "error": str(e),
                "content": response.text[:500],
            }

    except requests.exceptions.Timeout:
        print("⏰ Request timeout (30s)")
        return {"status": "timeout"}
    except requests.exceptions.RequestException as e:
        print(f"🌐 Network error: {e}")
        return {"status": "network_error", "error": str(e)}


def main():
    """Test various PNCP endpoint scenarios"""

    print("=" * 80)
    print("🚀 PNCP API Debug Script")
    print("=" * 80)

    # Test 1: contratacoes_publicacao with modalidade 1 (January 2021)
    print("\n🧪 Test 1: contratacoes_publicacao with modalidade 1 (Jan 2021)")
    result1 = test_pncp_endpoint(
        "/v1/contratacoes/publicacao",
        {
            "tamanhoPagina": 50,
            "pagina": 1,
            "dataInicial": "20210101",
            "dataFinal": "20210131",
            "codigoModalidadeContratacao": 1,
        },
    )

    # Test 2: Try a more recent date (December 2024)
    print("\n🧪 Test 2: contratacoes_publicacao with modalidade 1 (Dec 2024)")
    result2 = test_pncp_endpoint(
        "/v1/contratacoes/publicacao",
        {
            "tamanhoPagina": 50,
            "pagina": 1,
            "dataInicial": "20241201",
            "dataFinal": "20241231",
            "codigoModalidadeContratacao": 1,
        },
    )

    # Test 3: Try different modalidade (6 = Pregão Eletrônico)
    print("\n🧪 Test 3: contratacoes_publicacao with modalidade 6 (Dec 2024)")
    result3 = test_pncp_endpoint(
        "/v1/contratacoes/publicacao",
        {
            "tamanhoPagina": 50,
            "pagina": 1,
            "dataInicial": "20241201",
            "dataFinal": "20241231",
            "codigoModalidadeContratacao": 6,
        },
    )

    # Test 4: Try contratos endpoint (no modalidade required)
    print("\n🧪 Test 4: contratos endpoint (no modalidade - Dec 2024)")
    result4 = test_pncp_endpoint(
        "/v1/contratos",
        {
            "tamanhoPagina": 500,
            "pagina": 1,
            "dataInicial": "20241201",
            "dataFinal": "20241231",
        },
    )

    # Test 5: Check if the API has data for recent dates
    print("\n🧪 Test 5: contratacoes_publicacao endpoint (Jan 2024)")
    result5 = test_pncp_endpoint(
        "/v1/contratacoes/publicacao",
        {
            "tamanhoPagina": 50,
            "pagina": 1,
            "dataInicial": "20240101",
            "dataFinal": "20240131",
            "codigoModalidadeContratacao": 6,
        },
    )

    print("\n" + "=" * 80)
    print("📊 SUMMARY")
    print("=" * 80)

    tests = [
        ("contratacoes_publicacao modalidade=1 (Jan 2021)", result1),
        ("contratacoes_publicacao modalidade=1 (Dec 2024)", result2),
        ("contratacoes_publicacao modalidade=6 (Dec 2024)", result3),
        ("contratos (no modalidade - Dec 2024)", result4),
        ("contratacoes_publicacao (Jan 2024)", result5),
    ]

    for name, result in tests:
        status = result.get("status", "unknown")
        if status == "success":
            data_count = (
                len(result["data"].get("data", [])) if isinstance(result["data"], dict) else 0
            )
            print(f"✅ {name}: SUCCESS ({data_count} records)")
        elif status == "empty":
            print(f"📭 {name}: EMPTY (204 No Content)")
        else:
            print(f"❌ {name}: {status.upper()}")

    print("\n💡 Recommendations:")
    print("- If all tests return 204 (No Content), the API may not have data for those date ranges")
    print("- If JSON errors occur, there might be API response format issues")
    print("- Try different date ranges or modalidades to find data")
    print("- Consider checking PNCP documentation for data availability periods")


if __name__ == "__main__":
    main()
