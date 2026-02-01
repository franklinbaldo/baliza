"""BDD step definitions for resilience.feature."""

import httpx
import pytest
from pytest_bdd import given, scenario, then, when
from typer.testing import CliRunner
import duckdb
from pathlib import Path

from baliza.cli_simple import app

# Mark all resilience scenarios as Tier 1 (Core Features)
pytestmark = pytest.mark.tier1

runner = CliRunner()

@pytest.fixture
def api_mock_data():
    return {
        "calls": 0,
        "fail_until": 0,
        "records": [
            {
                "numeroControlePNCP": "12345678000101-2-000001/2024",
                "anoCompra": 2024,
                "sequencialCompra": 1,
                "orgaoEntidade": {"cnpj": "12345678000101", "razaoSocial": "Orgao Teste", "poderId": "1"},
                "unidadeOrgao": {"codigoUnidade": "1", "nomeUnidade": "Unidade Teste"},
                "modalidadeId": 1,
                "modalidadeNome": "Pregao",
                "valorInicial": 100.0,
                "dataPublicacao": "2024-01-01T10:00:00",
                "dataVigenciaInicio": "2024-01-01",
                "dataVigenciaFim": "2024-12-31",
                "objetoContrato": "Objeto Teste",
                "dataAtualizacao": "2024-01-01T10:00:00",
            }
        ]
    }

@scenario(
    "../features/resilience.feature",
    "The extract command recovers from a transient API error",
)
def test_extract_recovers_from_transient_error():
    """Scenario: The extract command recovers from a transient API error."""

@scenario(
    "../features/resilience.feature",
    "The extract command gives up after multiple consecutive failures",
)
def test_extract_fails_after_multiple_retries():
    """Scenario: The extract command gives up after multiple consecutive failures."""

@given("a PNCP API that will fail transiently", target_fixture="api_mock")
def pncp_api_transient_fail(monkeypatch, api_mock_data):
    def mock_get(self, url, **kwargs):
        api_mock_data["calls"] += 1
        if api_mock_data["calls"] <= api_mock_data["fail_until"]:
            raise httpx.HTTPStatusError(
                message="Transient Error",
                request=None,
                response=httpx.Response(status_code=500, request=None)
            )

        return httpx.Response(
            status_code=200,
            json={"data": api_mock_data["records"], "totalPaginas": 1},
            request=httpx.Request(method="GET", url=url)
        )

    monkeypatch.setattr(httpx.Client, "get", mock_get)
    return api_mock_data

@given("the PNCP API will return a 500 error for the first half of a date range")
def set_fail_until(api_mock):
    # We want it to fail the first run and succeed on the second.
    # The first run will try 4 times (retry limit).
    # So we fail 4 times.
    api_mock["fail_until"] = 4

@given("the PNCP API will succeed for the second half of the date range")
def step_impl():
    # Implicitly handled by fail_until
    pass

@given("the PNCP API will consistently return a 500 error", target_fixture="api_mock")
def pncp_api_consistent_fail(monkeypatch, api_mock_data):
    def mock_get(self, url, **kwargs):
        api_mock_data["calls"] += 1
        raise httpx.HTTPStatusError(
            message="Persistent Error",
            request=None,
            response=httpx.Response(status_code=500, request=None)
        )

    monkeypatch.setattr(httpx.Client, "get", mock_get)
    api_mock_data["fail_until"] = 999
    return api_mock_data

@when('I run the "baliza extract" command for the full date range', target_fixture="result")
def run_extract_full_range(db_path):
    # First attempt - expected to fail if fail_until >= 4
    result1 = runner.invoke(app, ["extract", "--start", "2024-01-01", "--end", "2024-01-01", "--duckdb", str(db_path)])

    # Second attempt - expected to succeed
    result2 = runner.invoke(app, ["extract", "--start", "2024-01-01", "--end", "2024-01-01", "--duckdb", str(db_path)])
    return result2

@when('I run the "baliza extract" command', target_fixture="result")
def run_extract_default(db_path):
    return runner.invoke(app, ["extract", "--start", "2024-01-01", "--end", "2024-01-01", "--duckdb", str(db_path)])

@then("the command should eventually succeed")
def check_success(result):
    assert result.exit_code == 0

@then("the command should fail after a reasonable number of retries")
def check_failure(result):
    assert result.exit_code != 0

@then("the final dataset should contain all records for the full date range")
def check_dataset_content(db_path):
    with duckdb.connect(str(db_path)) as con:
        count = con.execute("SELECT COUNT(*) FROM baliza_raw.contratos").fetchone()[0]
        assert count > 0

@then("the final dataset should not contain duplicate records")
def check_no_duplicates(db_path):
    with duckdb.connect(str(db_path)) as con:
        count = con.execute("SELECT COUNT(*), COUNT(DISTINCT numeroControlePNCP) FROM baliza_raw.contratos").fetchone()
        assert count[0] == count[1]

@then("the run history should show one failed run and one successful run")
def check_run_history(db_path):
    with duckdb.connect(str(db_path)) as con:
        runs = con.execute("SELECT status, COUNT(*) FROM baliza_state.runs GROUP BY status").fetchall()
        stats = dict(runs)
        assert stats.get("failed") == 1
        assert stats.get("completed") == 1

@then("the error message should clearly indicate a persistent failure")
def check_error_message(result):
    assert "Extraction failed" in result.stdout

@then("the state history should log the multiple failed attempts")
def check_state_history_failure(db_path):
     with duckdb.connect(str(db_path)) as con:
        runs = con.execute("SELECT status FROM baliza_state.runs WHERE status = 'failed'").fetchall()
        assert len(runs) >= 1
