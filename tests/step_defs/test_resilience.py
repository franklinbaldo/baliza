"""BDD step definitions for resilience.feature."""

import httpx
import pytest
from pytest_bdd import given, scenario, then, when
from typer.testing import CliRunner
from baliza.cli_simple import app
from pathlib import Path

# Mark all resilience scenarios as Tier 1 (Core Features)
pytestmark = pytest.mark.tier1

runner = CliRunner()

@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Create a clean DuckDB database."""
    return tmp_path / "resilience.duckdb"

# =============================================================================
# Scenario: The extract command recovers from a transient API error
# =============================================================================

@scenario(
    "../features/resilience.feature",
    "The extract command recovers from a transient API error",
)
def test_extract_recovers_from_transient_error():
    """Scenario: The extract command recovers from a transient API error."""
    pass

@given("a PNCP API that will fail transiently", target_fixture="api_mock")
def api_mock(monkeypatch):
    calls = {"count": 0}

    def mock_get(self, url, params=None, **kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            # Return a 500 error on first call
            resp = httpx.Response(500, request=httpx.Request("GET", url))
            return resp
        else:
            # Success on subsequent calls
            data = {
                "data": [{"numeroControlePNCP": "123-1-000001/2024", "valorInicial": 100.0}],
                "totalPaginas": 1,
                "totalRegistros": 1,
                "numeroPagina": 1
            }
            resp = httpx.Response(200, json=data, request=httpx.Request("GET", url))
            return resp

    monkeypatch.setattr(httpx.Client, "get", mock_get)
    return calls

@when('I run the "baliza extract" command for the full date range', target_fixture="result")
def run_extract(db_path):
    result = runner.invoke(app, ["extract", "--start", "2024-01-01", "--end", "2024-01-01", "--duckdb", str(db_path)])
    return result

@then("the command should eventually succeed")
def check_success(result):
    assert result.exit_code == 0
    assert "Extraction Complete" in result.stdout

@then("the final dataset should contain all records for the full date range")
def check_data(db_path):
    import duckdb
    with duckdb.connect(str(db_path)) as con:
        count = con.execute("SELECT COUNT(*) FROM baliza_raw.contratos").fetchone()[0]
        assert count == 1

# =============================================================================
# Scenario: The extract command gives up after multiple consecutive failures
# =============================================================================

@scenario(
    "../features/resilience.feature",
    "The extract command gives up after multiple consecutive failures",
)
def test_extract_fails_after_multiple_retries():
    """Scenario: The extract command gives up after multiple consecutive failures."""
    pass

@given("the PNCP API will consistently return a 500 error")
def api_fail_consistently(monkeypatch):
    def mock_get(self, url, params=None, **kwargs):
        return httpx.Response(500, request=httpx.Request("GET", url))
    monkeypatch.setattr(httpx.Client, "get", mock_get)

@when('I run the "baliza extract" command', target_fixture="result_fail")
def run_extract_fail(db_path):
    # Reduce retry wait time for tests if possible, but here we just wait
    result = runner.invoke(app, ["extract", "--start", "2024-01-01", "--end", "2024-01-01", "--duckdb", str(db_path)])
    return result

@then("the command should fail after a reasonable number of retries")
def check_failure(result_fail):
    assert result_fail.exit_code != 0
    assert "Extraction failed" in result_fail.stdout

@then("the error message should clearly indicate a persistent failure")
def check_error_message(result_fail):
    assert "500" in result_fail.stdout or "Error" in result_fail.stdout
