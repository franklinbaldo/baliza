"""BDD step definitions for resilience.feature."""

from pathlib import Path

import duckdb
import httpx
import pytest
from pytest_bdd import given, scenario, then, when
from typer.testing import CliRunner

from baliza.cli_simple import app

# Mark all resilience scenarios as Tier 1 (Core Features)
pytestmark = pytest.mark.tier1

runner = CliRunner()

# --- Fixtures ---

@pytest.fixture
def api_mock_state():
    return {"fail_count": 0, "max_fails": 0}

# --- Background Steps ---

@given("a clean local data store", target_fixture="db_path")
def clean_db(tmp_path: Path):
    db = tmp_path / "resilience.duckdb"
    if db.exists():
        db.unlink()
    return db

@given("a PNCP API that will fail transiently")
def setup_transient_api(monkeypatch, api_mock_state):
    def mock_get(self, url, **kwargs):
        if api_mock_state["fail_count"] < api_mock_state["max_fails"]:
            api_mock_state["fail_count"] += 1
            return httpx.Response(500, content="Internal Server Error", request=httpx.Request("GET", url))

        # Success response
        record = {
            "numeroControlePNCP": f"RES-{api_mock_state['fail_count']}",
            "dataPublicacao": "2024-01-01T10:00:00",
        }
        return httpx.Response(200, json={"data": [record], "totalPaginas": 1}, request=httpx.Request("GET", url))

    monkeypatch.setattr(httpx.Client, "get", mock_get)

# --- Scenario: The extract command recovers from a transient API error ---

@scenario(
    "../features/resilience.feature",
    "The extract command recovers from a transient API error",
)
def test_extract_recovers_from_transient_error():
    pass

@given("the PNCP API will return a 500 error for the first half of a date range")
def set_first_half_fail(api_mock_state):
    # In this context, let's say it fails for the first 2 calls (within one run's retries or across runs)
    # The simple extractor currently retries 4 times.
    # To simulate across-run failure, we need to fail more than 4 times.
    api_mock_state["max_fails"] = 5

@given("the PNCP API will succeed for the second half of the date range")
def set_second_half_success():
    # This is implicitly handled by setup_transient_api once fail_count >= max_fails
    pass

@when('I run the "baliza extract" command for the full date range', target_fixture="run_results")
def run_extract_twice(db_path, api_mock_state):
    # First run: should fail because max_fails (5) > tenacity retries (4)
    result1 = runner.invoke(app, ["extract", "--start", "2024-01-01", "--end", "2024-01-01", "--duckdb", str(db_path)])

    # Second run: should succeed because fail_count is already 5 (if tenacity retries 4 times and fails, then next run starts with fail_count=4? No, state is shared)
    # Wait, tenacity retries are INSIDE the extract call.
    # If it fails 4 times in result1, fail_count becomes 4.
    # In second run, first call will be 5th fail, then success.
    result2 = runner.invoke(app, ["extract", "--start", "2024-01-01", "--end", "2024-01-01", "--duckdb", str(db_path)])

    return [result1, result2]

@then("the command should eventually succeed")
def check_eventual_success(run_results):
    assert run_results[0].exit_code != 0
    assert run_results[1].exit_code == 0

@then("the final dataset should contain all records for the full date range")
def check_final_dataset(db_path):
    with duckdb.connect(str(db_path)) as con:
        count = con.execute("SELECT COUNT(*) FROM baliza_raw.contratos").fetchone()[0]
        assert count >= 1

@then("the final dataset should not contain duplicate records")
def check_no_duplicates(db_path):
    with duckdb.connect(str(db_path)) as con:
        count = con.execute("SELECT COUNT(DISTINCT numeroControlePNCP) FROM baliza_raw.contratos").fetchone()[0]
        total = con.execute("SELECT COUNT(*) FROM baliza_raw.contratos").fetchone()[0]
        assert count == total

@then("the run history should show one failed run and one successful run")
def check_run_history(db_path):
    with duckdb.connect(str(db_path)) as con:
        history = con.execute("SELECT status FROM baliza_state.runs ORDER BY started_at").fetchall()
        assert len(history) >= 2
        # First attempt failed, second succeeded
        # Wait, if first attempt retried 4 times, it's one "run" that failed.
        # Second attempt is another "run" that succeeded.
        assert history[0][0] == 'failed'
        assert history[1][0] == 'completed'

# --- Scenario: The extract command gives up after multiple consecutive failures ---

@scenario(
    "../features/resilience.feature",
    "The extract command gives up after multiple consecutive failures",
)
def test_extract_fails_after_multiple_retries():
    pass

@given("the PNCP API will consistently return a 500 error")
def set_consistent_fail(api_mock_state):
    api_mock_state["max_fails"] = 100

@when('I run the "baliza extract" command', target_fixture="single_run_result")
def run_extract_once(db_path):
    return runner.invoke(app, ["extract", "--start", "2024-01-01", "--end", "2024-01-01", "--duckdb", str(db_path)])

@then("the command should fail after a reasonable number of retries")
def check_fail_retries(single_run_result):
    assert single_run_result.exit_code != 0

@then("the error message should clearly indicate a persistent failure")
def check_error_message(single_run_result):
    assert "Extraction failed" in single_run_result.stdout

@then("the state history should log the multiple failed attempts")
def check_state_history_fail(db_path):
    with duckdb.connect(str(db_path)) as con:
        history = con.execute("SELECT status FROM baliza_state.runs").fetchall()
        assert any(status[0] == 'failed' for status in history)
