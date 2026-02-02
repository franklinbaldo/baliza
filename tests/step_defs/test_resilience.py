"""BDD step definitions for resilience.feature."""

from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import httpx
import pytest
from pytest_bdd import given, scenario, then, when
from typer.testing import CliRunner

from baliza.cli_simple import app

runner = CliRunner()

# Mark all resilience scenarios as Tier 1 (Core Features)
pytestmark = pytest.mark.tier1


# =============================================================================
# Background Steps
# =============================================================================


@given("a clean local data store", target_fixture="db_path")
def clean_db(tmp_path: Path) -> Path:
    """Create a clean DuckDB database."""
    db = tmp_path / "test.duckdb"
    if db.exists():
        db.unlink()
    return db


@given("a PNCP API that will fail transiently", target_fixture="mock_api_state")
def mock_transient_api(monkeypatch):
    """Mock the PNCP API with support for transient and persistent failures."""
    state = {
        "failures_remaining": {},  # date -> count
        "persistent_failure": False,
        "call_count": 0,
    }

    def mock_get(self, url, **kwargs):
        state["call_count"] += 1
        params = kwargs.get("params", {})
        date_in = params.get("dataInicial")

        if state["persistent_failure"]:
            return httpx.Response(
                500, content="Persistent Error", request=httpx.Request("GET", url)
            )

        if date_in in state["failures_remaining"] and state["failures_remaining"][date_in] > 0:
            state["failures_remaining"][date_in] -= 1
            return httpx.Response(
                500, content="Transient Error", request=httpx.Request("GET", url)
            )

        # Generate data for all days in range
        start = datetime.strptime(date_in, "%Y%m%d")
        end = datetime.strptime(params.get("dataFinal", date_in), "%Y%m%d")
        data = []
        curr = start
        while curr <= end:
            d_str = curr.strftime("%Y%m%d")
            data.append(
                {
                    "numeroControlePNCP": f"P-{d_str}",
                    "dataPublicacao": curr.strftime("%Y-%m-%d"),
                }
            )
            curr += timedelta(days=1)

        return httpx.Response(
            200,
            json={"data": data, "totalPaginas": 1},
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr(httpx.Client, "get", mock_get)
    return state


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
def set_first_half_fail(mock_api_state):
    """Set the first day to fail more times than the retry limit (4)."""
    # 5 failures will exceed the 4 retries of Tenacity in a single run.
    mock_api_state["failures_remaining"]["20240101"] = 5


@given("the PNCP API will succeed for the second half of the date range")
def set_second_half_success(mock_api_state):
    """Second half will succeed by default."""
    pass


@when(
    'I run the "baliza extract" command for the full date range', target_fixture="extract_result"
)
def run_extract_resilient(db_path, mock_api_state):
    """Run extraction twice to simulate failure and resumption."""
    # Run 1: Expected to fail on the first day
    res1 = runner.invoke(
        app,
        [
            "extract",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-02",
            "--duckdb",
            str(db_path),
        ],
    )

    # Run 2: Expected to succeed (resumes from checkpoint)
    res2 = runner.invoke(
        app,
        [
            "extract",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-02",
            "--duckdb",
            str(db_path),
        ],
    )

    return {"res1": res1, "res2": res2, "db_path": db_path}


@then("the command should eventually succeed")
def check_eventual_success(extract_result):
    """Verify the second run succeeded."""
    assert extract_result["res2"].exit_code == 0


@then("the final dataset should contain all records for the full date range")
def check_full_data(extract_result):
    """Verify all expected records are in the database."""
    with duckdb.connect(str(extract_result["db_path"])) as con:
        count = con.execute("SELECT COUNT(*) FROM baliza_raw.contratos").fetchone()[0]
        assert count == 2


@then("the final dataset should not contain duplicate records")
def check_no_duplicates(extract_result):
    """Verify no duplicate records were created during resumption."""
    with duckdb.connect(str(extract_result["db_path"])) as con:
        count = con.execute(
            "SELECT COUNT(DISTINCT numeroControlePNCP) FROM baliza_raw.contratos"
        ).fetchone()[0]
        total = con.execute("SELECT COUNT(*) FROM baliza_raw.contratos").fetchone()[0]
        assert count == total


@then("the run history should show one failed run and one successful run")
def check_run_history(extract_result):
    """Verify both runs are recorded in the history."""
    with duckdb.connect(str(extract_result["db_path"])) as con:
        runs = con.execute(
            "SELECT status, COUNT(*) FROM baliza_state.runs GROUP BY status"
        ).fetchall()
        runs_dict = dict(runs)
        assert runs_dict.get("failed") == 1
        assert runs_dict.get("completed") == 1


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
def set_consistent_fail(mock_api_state):
    """Set the API to always fail."""
    mock_api_state["persistent_failure"] = True


@when('I run the "baliza extract" command', target_fixture="fail_result")
def run_extract_fail(db_path, mock_api_state):
    """Run extraction expected to fail."""
    result = runner.invoke(
        app,
        ["extract", "--start", "2024-01-01", "--end", "2024-01-01", "--duckdb", str(db_path)],
    )
    return {"result": result, "db_path": db_path}


@then("the command should fail after a reasonable number of retries")
def check_failed_after_retries(fail_result):
    """Verify the command failed."""
    assert fail_result["result"].exit_code != 0


@then("the error message should clearly indicate a persistent failure")
def check_error_msg(fail_result):
    """Verify error message presence."""
    assert "Extraction failed" in fail_result["result"].stdout


@then("the state history should log the multiple failed attempts")
def check_state_history_fail(fail_result):
    """Verify the failed run is logged."""
    with duckdb.connect(str(fail_result["db_path"])) as con:
        count = con.execute(
            "SELECT COUNT(*) FROM baliza_state.runs WHERE status = 'failed'"
        ).fetchone()[0]
        assert count >= 1
