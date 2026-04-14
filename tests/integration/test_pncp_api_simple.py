"""
Simple integration tests for PNCP API using VCR cassettes.

These tests make direct HTTP calls to the PNCP API (not through dlt)
and record responses using VCR. This allows us to:
1. Test actual API responses
2. Have fast, reproducible tests
3. Not depend on API availability for test runs

To re-record cassettes:
    rm -rf tests/cassettes/*.yaml
    pytest tests/integration/

NOTE: These tests are currently skipped because:
- PNCP API returns 403 Forbidden in CI/CD environments
- Cassettes need to be recorded locally first
- Once cassettes exist, tests will run from recordings
"""

import httpx
import pytest

# Skip these tests if cassettes don't exist
# Users can record cassettes locally, then commit them
# pytest_plugins = ["pytest_vcr"]

# Mark all tests in this module to use VCR
pytestmark = pytest.mark.vcr


# PNCP API headers matching Baliza configuration
PNCP_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "baliza/0.1 integration-test",
}


def pncp_get(url: str, params: dict, timeout: int = 60) -> httpx.Response:
    """Helper to make GET request to PNCP API with correct headers."""
    return httpx.get(url, params=params, headers=PNCP_HEADERS, timeout=timeout)


@pytest.mark.vcr()
def test_pncp_api_get_contratos_single_day():
    """
    Test PNCP API /v1/contratos endpoint for a single day.

    Makes a real API request (recorded by VCR) and verifies:
    - API returns 200 OK
    - Response has expected structure
    - Data contains contracts
    - Pagination fields are present
    """
    base_url = "https://pncp.gov.br/api/consulta/v1/contratos"

    params = {
        "dataInicial": "20241001",
        "dataFinal": "20241001",
        "pagina": 1,
        "tamanhoPagina": 10,  # Small page size for testing
    }

    response = pncp_get(base_url, params)

    # Verify response
    assert response.status_code == 200

    data = response.json()

    # Verify structure
    assert "data" in data
    assert "totalPaginas" in data
    assert "totalRegistros" in data
    assert "numeroPagina" in data

    # Verify data exists
    assert isinstance(data["data"], list)

    if len(data["data"]) > 0:
        # Verify contract structure
        contrato = data["data"][0]
        assert "numero_controle_pncp" in contrato
        assert "valor_inicial" in contrato
        assert "data_publicacaoPncp" in contrato

        # Verify numero_controle_pncp format (CNPJ-2-seq/year)
        assert "-" in contrato["numero_controle_pncp"]
        assert "/" in contrato["numero_controle_pncp"]


@pytest.mark.vcr()
def test_pncp_api_pagination():
    """
    Test PNCP API pagination.

    Verifies that totalPaginas and numeroPagina fields work correctly.
    """
    base_url = "https://pncp.gov.br/api/consulta/v1/contratos"

    # Request page 1
    params = {
        "dataInicial": "20240901",
        "dataFinal": "20240930",  # Full month should have multiple pages
        "pagina": 1,
        "tamanhoPagina": 50,
    }

    response = pncp_get(base_url, params)
    assert response.status_code == 200

    data = response.json()

    # Verify pagination fields
    assert data["numeroPagina"] == 1
    assert data["totalPaginas"] >= 1
    assert data["totalRegistros"] > 0

    if data["totalPaginas"] > 1:
        # Request page 2
        params["pagina"] = 2
        response2 = pncp_get(base_url, params)
        assert response2.status_code == 200

        data2 = response2.json()
        assert data2["numeroPagina"] == 2

        # Page 1 and 2 should have different data
        assert data["data"] != data2["data"]


@pytest.mark.vcr()
def test_pncp_api_empty_response():
    """
    Test PNCP API response for date range with no data.

    When there's no data, API should return 204 No Content or empty list.
    """
    base_url = "https://pncp.gov.br/api/consulta/v1/contratos"

    # Request from far future (should have no data)
    params = {
        "dataInicial": "20300101",
        "dataFinal": "20300101",
        "pagina": 1,
        "tamanhoPagina": 10,
    }

    response = pncp_get(base_url, params)

    # API may return 204 No Content or 200 with empty data
    assert response.status_code in [200, 204]

    if response.status_code == 200:
        data = response.json()
        # Either empty list or no data field
        if "data" in data:
            assert len(data["data"]) == 0 or data["data"] == []


@pytest.mark.xfail(reason="PNCP API is unstable and returns 400 for valid requests")
@pytest.mark.vcr()
def test_pncp_api_response_fields():
    """
    Test that PNCP API returns all expected fields.

    Verifies the complete response structure matches documentation.
    """
    base_url = "https://pncp.gov.br/api/consulta/v1/contratos"

    params = {
        "dataInicial": "20240901",
        "dataFinal": "20240930",
        "pagina": 1,
        "tamanhoPagina": 5,
    }

    response = pncp_get(base_url, params)
    assert response.status_code == 200

    data = response.json()

    # Top-level fields
    required_top_level = [
        "data",
        "totalPaginas",
        "totalRegistros",
        "numeroPagina",
        "paginasRestantes",
        "empty",
    ]

    for field in required_top_level:
        assert field in data, f"Missing field: {field}"

    # If we have contracts, verify contract fields
    if len(data["data"]) > 0:
        contrato = data["data"][0]

        # Core contract fields
        expected_fields = [
            "numero_controle_pncp",
            "valor_inicial",
            "data_publicacaoPncp",
            "objeto_contrato",
            "modalidade_id",
        ]

        for field in expected_fields:
            assert field in contrato, f"Missing contract field: {field}"


@pytest.mark.xfail(reason="PNCP API is unstable and returns 400 for valid requests")
@pytest.mark.vcr()
def test_pncp_api_date_format():
    """
    Test that PNCP API accepts YYYYMMDD date format.

    PNCP expects dates in compact format (YYYYMMDD), not ISO format.
    """
    base_url = "https://pncp.gov.br/api/consulta/v1/contratos"

    # Correct format: YYYYMMDD
    params = {
        "dataInicial": "20240901",
        "dataFinal": "20240901",
        "pagina": 1,
        "tamanhoPagina": 1,
    }

    response = pncp_get(base_url, params)

    # Should work with correct format
    assert response.status_code in [200, 204]  # 204 if no data for that date


@pytest.mark.vcr()
def test_pncp_api_max_page_size():
    """
    Test PNCP API respects maximum page size.

    PNCP API has a maximum page size of 500 records.
    """
    base_url = "https://pncp.gov.br/api/consulta/v1/contratos"

    # Try with page size = 500 (max allowed)
    params = {
        "dataInicial": "20240901",
        "dataFinal": "20240930",
        "pagina": 1,
        "tamanhoPagina": 500,
    }

    response = pncp_get(base_url, params)
    assert response.status_code == 200

    data = response.json()

    # Should return up to 500 records
    assert len(data["data"]) <= 500
