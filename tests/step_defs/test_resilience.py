"""BDD step definitions for resilience.feature."""

from pathlib import Path
from urllib.parse import parse_qs, urlparse

import duckdb
import httpx
import pytest
from pytest_bdd import given, scenario, then, when
from typer.testing import CliRunner

from baliza.cli_simple import app

pytestmark = pytest.mark.tier1

runner = CliRunner()

# Mark all resilience scenarios as Tier 1 (Core Features)


# =============================================================================
# Background Steps
# =============================================================================


@given("a clean local data store", target_fixture="db_path")
def db_path_fixture(tmp_path: Path) -> Path:
    """Create a clean DuckDB database."""
    db = tmp_path / "test_resilience.duckdb"
    if db.exists():
        db.unlink()
    return db


@given("a PNCP API that will fail transiently")
def setup_api_marker():
    """Marker for API setup."""
    pass


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


@given(
    "the PNCP API will return a 500 error for the first half of a date range",
    target_fixture="transient_mock",
)
def setup_transient_failure(httpx_mock):
    """Mock API to fail for the first day, then succeed when called again."""
    call_count = 0

    def mock_pncp_api(request: httpx.Request):
        nonlocal call_count
        call_count += 1

        parsed_url = urlparse(str(request.url))
        query_params = parse_qs(parsed_url.query)
        start_date = query_params.get("dataInicial", [None])[0]
        end_date = query_params.get("dataFinal", [None])[0]

        # Simulate transient failure for 20240101
        if start_date == "20240101" and end_date == "20240101":
            if call_count <= 4:  # First 4 attempts of first run fail
                return httpx.Response(500, content="Internal Server Error")
            else:
                return httpx.Response(
                    200, json={"data": [{"numeroControlePNCP": "PNCP-1"}], "totalPaginas": 1}
                )

        # Success for full range
        if start_date == "20240101" and end_date == "20240102":
            return httpx.Response(
                200,
                json={
                    "data": [{"numeroControlePNCP": "PNCP-1"}, {"numeroControlePNCP": "PNCP-2"}],
                    "totalPaginas": 1,
                },
            )

        return httpx.Response(200, json={"data": [], "totalPaginas": 0})

    httpx_mock.add_callback(mock_pncp_api, is_reusable=True)
    return httpx_mock


@given("the PNCP API will succeed for the second half of the date range")
def setup_success_marker():
    """Handled in setup_transient_failure."""
    pass


@when(
    'I run the "baliza extract" command for the full date range',
    target_fixture="extract_twice_result",
)
def run_extract_twice(db_path, transient_mock):
    """Run extraction twice. First fails, second succeeds."""
    # First call - only first day, expected to fail
    runner.invoke(
        app, ["extract", "--start", "2024-01-01", "--end", "2024-01-01", "--duckdb", str(db_path)]
    )
    # Second call - full range, expected to succeed
    result = runner.invoke(
        app, ["extract", "--start", "2024-01-01", "--end", "2024-01-02", "--duckdb", str(db_path)]
    )
    return result


@then("the command should eventually succeed")
def check_eventual_success(extract_twice_result):
    assert extract_twice_result.exit_code == 0


@then("the final dataset should contain all records for the full date range")
def check_records(db_path):
    with duckdb.connect(str(db_path)) as con:
        count = con.execute("SELECT COUNT(*) FROM baliza_raw.contratos").fetchone()[0]
        assert count == 2


@then("the final dataset should not contain duplicate records")
def check_no_dupes(db_path):
    with duckdb.connect(str(db_path)) as con:
        dupes = con.execute(
            "SELECT numeroControlePNCP, COUNT(*) FROM baliza_raw.contratos GROUP BY 1 HAVING COUNT(*) > 1"
        ).fetchall()
        assert not dupes


@then("the run history should show one failed run and one successful run")
def check_run_history(db_path):
    with duckdb.connect(str(db_path)) as con:
        history = con.execute("SELECT status FROM baliza_state.runs ORDER BY started_at").fetchall()
        assert len(history) >= 2
        assert history[0][0] == "failed"
        assert history[-1][0] == "completed"


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
def setup_persistent_failure(httpx_mock):
    httpx_mock.add_response(status_code=500, is_reusable=True)


@when('I run the "baliza extract" command', target_fixture="fail_result")
def run_extract_fail(db_path):
    result = runner.invoke(
        app, ["extract", "--start", "2024-01-01", "--end", "2024-01-01", "--duckdb", str(db_path)]
    )
    return result


@then("the command should fail after a reasonable number of retries")
def check_failure(fail_result):
    assert fail_result.exit_code != 0


@then("the error message should clearly indicate a persistent failure")
def check_error_message(fail_result):
    assert "Extraction failed" in fail_result.stdout


@then("the state history should log the multiple failed attempts")
def check_fail_history(db_path):
    with duckdb.connect(str(db_path)) as con:
        history = con.execute("SELECT status FROM baliza_state.runs").fetchall()
        assert len(history) >= 1
        assert history[-1][0] == "failed"
