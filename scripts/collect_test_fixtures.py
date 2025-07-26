#!/usr/bin/env python3
"""
Script to collect real test fixtures from PNCP API endpoints
Saves JSON responses to tests/fixtures/ for use in unit tests
"""

# FIXME: This script is largely obsolete and should be removed or completely rewritten.
#        With the migration to DLT, the primary method for testing API interactions
#        should involve mocking DLT's internal HTTP client (e.g., using `pytest-httpx`
#        to intercept requests made by DLT's `rest_api_source`).
#        
#        Reasons for obsolescence/removal:
#        1.  **Legacy HTTP Client:** It relies on `PNCPClient`, `URLBuilder`, and
#            `DateRangeHelper`, which are part of the old, pre-DLT HTTP client
#            implementation. DLT now handles all HTTP requests internally.
#        2.  **Duplicated Logic:** The core logic for fetching and saving fixtures
#            is duplicated across multiple functions, making it hard to maintain.
#        3.  **Misaligned Testing Strategy:** If E2E tests are rewritten to mock
#            DLT's behavior, these pre-collected JSON files might not be directly
#            used or necessary.
#        4.  **Maintenance Overhead:** Keeping this script updated with API changes
#            and new endpoints is an unnecessary overhead if DLT handles the API
#            interaction.
#
#        Recommendation:
#        -   **Option 1 (Preferred):** Delete this script entirely. Focus on writing
#            new, DLT-native tests that mock at the appropriate layer.
#        -   **Option 2 (If absolutely necessary):** Rewrite this script from scratch
#            to use DLT's `rest_api_source` to fetch data, and then extract the raw
#            JSON payloads from DLT's internal structures. This would still require
#            significant refactoring and a clear justification for its existence.

import asyncio
import json
import sys
import os
from pathlib import Path

from baliza.utils.http_client import PNCPClient
from baliza.utils.endpoints import URLBuilder, DateRangeHelper
from baliza.enums import ModalidadeContratacao, get_enum_by_value
from baliza.config import settings

# Add src to path to import our modules
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

# Set PYTHONPATH for imports
os.environ["PYTHONPATH"] = str(project_root / "src")


async def collect_contratacoes_publicacao_fixture():
    """Collect real contratacoes/publicacao response"""
    print("🔍 Coletando fixture: contratacoes/publicacao...")

    client = PNCPClient()
    builder = URLBuilder()

    # Get recent date range (last 30 days to ensure data)
    data_inicial, data_final = DateRangeHelper.get_last_n_days(30)

    # Use first high priority modalidade
    modalidade = get_enum_by_value(
        ModalidadeContratacao, settings.HIGH_PRIORITY_MODALIDADES[0]
    )

    url = builder.build_contratacoes_publicacao_url(
        data_inicial=data_inicial,
        data_final=data_final,
        modalidade=modalidade,
        pagina=1,
    )

    try:
        api_request = await client.fetch_data(url, endpoint="contratacoes_publicacao")

        # Decompress and parse response
        import zlib

        response_json = json.loads(
            zlib.decompress(api_request.payload_compressed).decode("utf-8")
        )

        # Save to fixture file
        fixture_path = (
            Path(__file__).parent.parent
            / "tests/fixtures/contratacoes_publicacao_response.json"
        )
        fixture_path.parent.mkdir(parents=True, exist_ok=True)

        with open(fixture_path, "w", encoding="utf-8") as f:
            json.dump(response_json, f, indent=2, ensure_ascii=False)

        print(f"✅ Fixture salvo: {fixture_path}")
        print(f"   - {len(response_json.get('data', []))} registros")
        print(f"   - Total disponível: {response_json.get('totalRegistros', 0)}")

        await client.close()
        return True

    except Exception as e:
        print(f"❌ Erro ao coletar contratacoes/publicacao: {e}")
        await client.close()
        return False


async def collect_contratos_fixture():
    """Collect real contratos response"""
    print("🔍 Coletando fixture: contratos...")

    client = PNCPClient()
    builder = URLBuilder()

    # Get recent date range
    data_inicial, data_final = DateRangeHelper.get_last_n_days(30)

    url = builder.build_contratos_url(
        data_inicial=data_inicial, data_final=data_final, pagina=1
    )

    try:
        api_request = await client.fetch_data(url, endpoint="contratos")

        # Decompress and parse response
        import zlib

        response_json = json.loads(
            zlib.decompress(api_request.payload_compressed).decode("utf-8")
        )

        # Save to fixture file
        fixture_path = (
            Path(__file__).parent.parent / "tests/fixtures/contratos_response.json"
        )

        with open(fixture_path, "w", encoding="utf-8") as f:
            json.dump(response_json, f, indent=2, ensure_ascii=False)

        print(f"✅ Fixture salvo: {fixture_path}")
        print(f"   - {len(response_json.get('data', []))} registros")
        print(f"   - Total disponível: {response_json.get('totalRegistros', 0)}")

        await client.close()
        return True

    except Exception as e:
        print(f"❌ Erro ao coletar contratos: {e}")
        await client.close()
        return False


async def collect_atas_fixture():
    """Collect real atas response"""
    print("🔍 Coletando fixture: atas...")

    client = PNCPClient()
    builder = URLBuilder()

    # Get recent date range
    data_inicial, data_final = DateRangeHelper.get_last_n_days(30)

    url = builder.build_atas_url(
        data_inicial=data_inicial, data_final=data_final, pagina=1
    )

    try:
        api_request = await client.fetch_data(url, endpoint="atas")

        # Decompress and parse response
        import zlib

        response_json = json.loads(
            zlib.decompress(api_request.payload_compressed).decode("utf-8")
        )

        # Save to fixture file
        fixture_path = (
            Path(__file__).parent.parent / "tests/fixtures/atas_response.json"
        )

        with open(fixture_path, "w", encoding="utf-8") as f:
            json.dump(response_json, f, indent=2, ensure_ascii=False)

        print(f"✅ Fixture salvo: {fixture_path}")
        print(f"   - {len(response_json.get('data', []))} registros")
        print(f"   - Total disponível: {response_json.get('totalRegistros', 0)}")

        await client.close()
        return True

    except Exception as e:
        print(f"❌ Erro ao coletar atas: {e}")
        await client.close()
        return False


async def collect_orgaos_fixture():
    """Collect real orgaos response"""
    print("🔍 Coletando fixture: orgaos...")

    client = PNCPClient()

    # Orgaos endpoint doesn't need date parameters
    url = "https://pncp.gov.br/api/consulta/v1/orgaos?pagina=1"

    try:
        api_request = await client.fetch_data(url, endpoint="orgaos")

        # Decompress and parse response
        import zlib

        response_json = json.loads(
            zlib.decompress(api_request.payload_compressed).decode("utf-8")
        )

        # Save to fixture file
        fixture_path = (
            Path(__file__).parent.parent / "tests/fixtures/orgaos_response.json"
        )

        with open(fixture_path, "w", encoding="utf-8") as f:
            json.dump(response_json, f, indent=2, ensure_ascii=False)

        print(f"✅ Fixture salvo: {fixture_path}")
        print(f"   - {len(response_json.get('data', []))} registros")
        print(f"   - Total disponível: {response_json.get('totalRegistros', 0)}")

        await client.close()
        return True

    except Exception as e:
        print(f"❌ Erro ao coletar orgaos: {e}")
        await client.close()
        return False


async def collect_all_fixtures():
    """Collect all test fixtures from PNCP API"""
    print("🚀 Iniciando coleta de fixtures de teste...")
    print("📅 Usando período: últimos 30 dias")
    print(f"🎯 Modalidades prioritárias: {settings.HIGH_PRIORITY_MODALIDADES}")
    print()

    results = []

    # Collect each endpoint fixture
    results.append(await collect_contratacoes_publicacao_fixture())
    results.append(await collect_contratos_fixture())
    results.append(await collect_atas_fixture())
    results.append(await collect_orgaos_fixture())

    print()
    successful = sum(results)
    total = len(results)

    if successful == total:
        print(f"🎉 Todos os {total} fixtures coletados com sucesso!")
    else:
        print(f"⚠️  {successful}/{total} fixtures coletados com sucesso")
        return 1

    print()
    print("📁 Fixtures salvos em tests/fixtures/:")
    fixture_dir = Path(__file__).parent.parent / "tests/fixtures"
    for fixture_file in fixture_dir.glob("*.json"):
        size_kb = fixture_file.stat().st_size / 1024
        print(f"   - {fixture_file.name} ({size_kb:.1f} KB)")

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(collect_all_fixtures())
    sys.exit(exit_code)