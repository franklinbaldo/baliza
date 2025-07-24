#!/usr/bin/env python3
"""
Simple script to collect real test fixtures from PNCP API
Uses only built-in libraries to avoid dependency issues
"""

import json
import urllib.request
import urllib.parse
import gzip
from pathlib import Path
from datetime import date, timedelta


def get_last_n_days(n_days: int = 30):
    """Get date range for last N days"""
    end_date = date.today() - timedelta(days=1)  # Yesterday
    start_date = end_date - timedelta(days=n_days)
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


def fetch_api_data(url: str, endpoint_name: str):
    """Fetch data from PNCP API endpoint"""
    print(f"🔍 Coletando: {endpoint_name}")
    print(f"   URL: {url}")

    try:
        # Create request with proper headers
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "Baliza-Test-Collector/1.0")
        req.add_header("Accept", "application/json")

        # Make request
        with urllib.request.urlopen(req, timeout=30) as response:
            # Handle gzipped responses
            if response.headers.get("Content-Encoding") == "gzip":
                data = gzip.decompress(response.read())
            else:
                data = response.read()

            # Parse JSON
            response_json = json.loads(data.decode("utf-8"))

            # Save fixture
            fixture_path = (
                Path(__file__).parent.parent
                / f"tests/fixtures/{endpoint_name}_response.json"
            )
            fixture_path.parent.mkdir(parents=True, exist_ok=True)

            with open(fixture_path, "w", encoding="utf-8") as f:
                json.dump(response_json, f, indent=2, ensure_ascii=False)

            print(f"✅ Fixture salvo: {fixture_path.name}")
            print(f"   - {len(response_json.get('data', []))} registros")
            print(f"   - Total disponível: {response_json.get('totalRegistros', 0)}")
            print()

            return True

    except Exception as e:
        print(f"❌ Erro ao coletar {endpoint_name}: {e}")
        print()
        return False


def main():
    """Collect all test fixtures"""
    print("🚀 Coletando fixtures de teste do PNCP API...")

    # Get date range
    data_inicial, data_final = get_last_n_days(30)
    print(f"📅 Período: {data_inicial} até {data_final}")
    print()

    base_url = "https://pncp.gov.br/api/consulta/v1"

    # Define endpoints to collect
    endpoints = [
        {
            "name": "contratacoes_publicacao",
            "url": f"{base_url}/contratacoes/publicacao?dataInicial={data_inicial}&dataFinal={data_final}&codigoModalidadeContratacao=1&pagina=1",
        },
        {
            "name": "contratos",
            "url": f"{base_url}/contratos?dataInicial={data_inicial}&dataFinal={data_final}&pagina=1",
        },
        {
            "name": "atas",
            "url": f"{base_url}/atas?dataInicial={data_inicial}&dataFinal={data_final}&pagina=1",
        },
        {"name": "orgaos", "url": f"{base_url}/orgaos?pagina=1"},
    ]

    # Collect fixtures
    results = []
    for endpoint in endpoints:
        success = fetch_api_data(endpoint["url"], endpoint["name"])
        results.append(success)

    # Summary
    successful = sum(results)
    total = len(results)

    if successful == total:
        print(f"🎉 Todos os {total} fixtures coletados com sucesso!")
    else:
        print(f"⚠️  {successful}/{total} fixtures coletados com sucesso")

    print()
    print("📁 Fixtures salvos em tests/fixtures/:")
    fixture_dir = Path(__file__).parent.parent / "tests/fixtures"
    if fixture_dir.exists():
        for fixture_file in fixture_dir.glob("*.json"):
            size_kb = fixture_file.stat().st_size / 1024
            print(f"   - {fixture_file.name} ({size_kb:.1f} KB)")

    return 0 if successful == total else 1


if __name__ == "__main__":
    exit(main())
