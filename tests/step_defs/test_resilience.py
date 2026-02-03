"""BDD step definitions for resilience.feature."""


import duckdb
import httpx
import pytest
from pytest_bdd import given, scenario, then, when
from typer.testing import CliRunner

from baliza.cli_simple import app

# Mark all resilience scenarios as Tier 1 (Core Features)
pytestmark = pytest.mark.tier1

runner = CliRunner()

def create_mock_record(id="1"):
    return {
        "numeroControlePNCP": f"12345678000101-1-00000{id}/2024",
        "anoCompra": 2024,
        "sequencialCompra": int(id),
        "orgaoEntidade": {
            "cnpj": "12345678000101",
            "razaoSocial": "Orgao Teste",
            "poderId": "1"
        },
        "unidadeOrgao": {
            "codigoUnidade": "12345",
            "nomeUnidade": "Unidade Teste"
        },
        "modalidadeId": 1,
        "modalidadeNome": "Pregao",
        "valorInicial": 1000.0,
        "dataPublicacao": "2024-01-01T10:00:00",
        "dataVigenciaInicio": "2024-01-01T10:00:00",
        "dataVigenciaFim": "2024-12-31T23:59:59",
        "objetoContrato": "Objeto Teste",
        "informacaoComplementar": None,
        "numeroProcesso": "001/2024",
        "linkSistemaOrigem": "http://teste.com",
        "dataInclusao": "2024-01-01T10:00:00",
        "dataAtualizacao": "2024-01-01T10:00:00",
        "usuarioNome": "Usuario Teste"
    }

class MockAPIState:
    def __init__(self):
        self.failure_count = 0
        self.max_failures = 0
        self.total_calls = 0
        self.consistent_failure = False

@pytest.fixture
def mock_state():
    return MockAPIState()

# =============================================================================
# Background Steps
# =============================================================================

# 'a clean local data store' is implemented in tests/conftest.py

@given("a PNCP API that will fail transiently")
def pncp_api_fail_transiently(monkeypatch, mock_state):
    def mock_get(self, url, **kwargs):
        mock_state.total_calls += 1
        if mock_state.consistent_failure or mock_state.failure_count < mock_state.max_failures:
            mock_state.failure_count += 1
            return httpx.Response(500, request=httpx.Request("GET", url))

        return httpx.Response(
            200,
            json={"data": [create_mock_record()], "totalPaginas": 1},
            request=httpx.Request("GET", url)
        )
    monkeypatch.setattr(httpx.Client, "get", mock_get)

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


@given("the PNCP API will return a 500 error for the first half of a date range")
def api_fail_first_half(mock_state):
    # We'll simulate this by failing enough times to make the first command run fail
    # PNCPExtractor retries 4 times.
    mock_state.max_failures = 5


@given("the PNCP API will succeed for the second half of the date range")
def api_succeed_second_half(mock_state):
    # After 5 failures, it will succeed.
    pass


@when('I run the "baliza extract" command for the full date range')
def run_extract_full_range(db_path, mock_state):
    # First attempt: should fail because max_failures=5 and tenacity retries 4 times (total 4 attempts)
    # Wait, stop_after_attempt(4) means total 4 attempts.
    # So if it fails 4 times, the command fails.
    result1 = runner.invoke(
        app,
        [
            "extract",
            "--start", "2024-01-01",
            "--end", "2024-01-02",
            "--duckdb", str(db_path),
        ],
    )
    assert result1.exit_code != 0

    # Second attempt: should succeed because failure_count is now 4, and max_failures is 5.
    # Wait, if failure_count reached 4 in first run. Next call to mock_get will be 5th call.
    # It will fail one more time, then succeed.
    # Tenacity will retry and succeed on the 2nd attempt of the 2nd run.
    result2 = runner.invoke(
        app,
        [
            "extract",
            "--start", "2024-01-01",
            "--end", "2024-01-02",
            "--duckdb", str(db_path),
        ],
    )
    assert result2.exit_code == 0


@then("the command should eventually succeed")
def command_eventually_succeed():
    # Already asserted in @when
    pass


@then("the final dataset should contain all records for the full date range")
def check_dataset_complete(db_path):
    with duckdb.connect(str(db_path)) as con:
        count = con.execute("SELECT COUNT(*) FROM baliza_raw.contratos").fetchone()[0]
        assert count > 0


@then("the final dataset should not contain duplicate records")
def check_no_duplicates(db_path):
    with duckdb.connect(str(db_path)) as con:
        duplicates = con.execute("""
            SELECT numeroControlePNCP, COUNT(*)
            FROM baliza_raw.contratos
            GROUP BY 1 HAVING COUNT(*) > 1
        """).fetchall()
        assert not duplicates


@then("the run history should show one failed run and one successful run")
def check_run_history(db_path):
    with duckdb.connect(str(db_path)) as con:
        # This will fail until we implement run tracking
        history = con.execute("SELECT status FROM baliza_state.runs ORDER BY started_at").fetchall()
        assert len(history) == 2
        assert history[0][0] == "failed"
        assert history[1][0] == "completed"


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
def api_consistently_fail(mock_state):
    mock_state.consistent_failure = True


@when('I run the "baliza extract" command')
def run_extract_fail(db_path):
    pytest.result = runner.invoke(
        app,
        [
            "extract",
            "--start", "2024-01-01",
            "--end", "2024-01-01",
            "--duckdb", str(db_path),
        ],
    )


@then("the command should fail after a reasonable number of retries")
def command_should_fail():
    assert pytest.result.exit_code != 0


@then("the error message should clearly indicate a persistent failure")
def check_error_message():
    assert "Extraction failed" in pytest.result.stdout


@then("the state history should log the multiple failed attempts")
def check_state_history_failures(db_path):
    with duckdb.connect(str(db_path)) as con:
        # This will fail until we implement run tracking
        history = con.execute("SELECT status FROM baliza_state.runs").fetchall()
        assert len(history) >= 1
        assert history[0][0] == "failed"
