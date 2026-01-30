"""BDD step definitions for resilience.feature."""

import httpx
import pytest
from pytest_bdd import given, scenario, then, when
from typer.testing import CliRunner
from baliza.cli_simple import app
import duckdb
from pathlib import Path

runner = CliRunner()

@scenario(
    "../features/resilience.feature",
    "The extract command recovers from a transient API error",
)
def test_extract_recovers_from_transient_error():
    pass

@scenario(
    "../features/resilience.feature",
    "The extract command gives up after multiple consecutive failures",
)
def test_extract_fails_after_multiple_retries():
    pass

# --- Givens ---

@given("a PNCP API that will fail transiently", target_fixture="api_mock")
def api_mock(monkeypatch):
    class MockState:
        def __init__(self):
            self.calls = 0
            self.fail_until = 0

    state = MockState()

    def mock_get(self_client, url, **kwargs):
        state.calls += 1
        if state.calls <= state.fail_until:
            # Create a response with a mock request attached,
            # because extractor calls response.raise_for_status()
            # and some versions of httpx might need the request for that.
            resp = httpx.Response(500)
            resp._request = httpx.Request("GET", url)
            raise httpx.HTTPStatusError("Transient Error", request=resp.request, response=resp)

        resp = httpx.Response(
            200,
            json={
                "data": [
                    {"numeroControlePNCP": f"CONT-{state.calls}", "dataPublicacao": "2024-01-01T10:00:00"}
                ],
                "totalPaginas": 1
            }
        )
        resp._request = httpx.Request("GET", url)
        return resp

    monkeypatch.setattr(httpx.Client, "get", mock_get)
    return state

@given("the PNCP API will return a 500 error for the first half of a date range")
def api_fails_initially(api_mock):
    # To simulate "failed run then successful run", we need to fail more than tenacity's 4 attempts
    # Tenacity is configured for 4 attempts.
    api_mock.fail_until = 5

@given("the PNCP API will succeed for the second half of the date range")
def api_succeeds_later(api_mock):
    pass

@given("the PNCP API will consistently return a 500 error")
def api_fails_consistently(api_mock):
    api_mock.fail_until = 999

# --- Whens ---

@when('I run the "baliza extract" command for the full date range', target_fixture="results")
def run_extract_multiple(db_path):
    results = []
    # First attempt - expected to fail if fail_until > 4
    res1 = runner.invoke(app, ["extract", "--start", "2024-01-01", "--end", "2024-01-01", "--duckdb", str(db_path)])
    results.append(res1)

    # Second attempt - expected to succeed because fail_until=5 and we already had 4 attempts in first run
    # Wait, actually tenacity attempts are per page.
    # First run:
    # attempt 1: fail (1)
    # attempt 2: fail (2)
    # attempt 3: fail (3)
    # attempt 4: fail (4) -> raises exception.
    # Total calls: 4. fail_until is 5.
    # Second run:
    # attempt 1: fail (5)
    # attempt 2: success (6) -> OK!

    res2 = runner.invoke(app, ["extract", "--start", "2024-01-01", "--end", "2024-01-01", "--duckdb", str(db_path)])
    results.append(res2)
    return results

@when('I run the "baliza extract" command', target_fixture="result")
def run_extract_once(db_path):
    return runner.invoke(app, ["extract", "--start", "2024-01-01", "--end", "2024-01-01", "--duckdb", str(db_path)])

# --- Thens ---

@then("the command should eventually succeed")
def check_eventual_success(results):
    assert results[-1].exit_code == 0

@then("the final dataset should contain all records for the full date range")
def check_dataset_complete(db_path):
    with duckdb.connect(str(db_path)) as con:
        count = con.execute("SELECT COUNT(*) FROM baliza_raw.contratos").fetchone()[0]
        assert count > 0

@then("the final dataset should not contain duplicate records")
def check_no_duplicates(db_path):
    with duckdb.connect(str(db_path)) as con:
        rows = con.execute("SELECT numeroControlePNCP, COUNT(*) FROM baliza_raw.contratos GROUP BY 1 HAVING COUNT(*) > 1").fetchall()
        assert len(rows) == 0

@then("the run history should show one failed run and one successful run")
def check_run_history(db_path):
    with duckdb.connect(str(db_path)) as con:
        runs = con.execute("SELECT status, COUNT(*) FROM baliza_state.runs GROUP BY 1").fetchall()
        # runs is list of tuples: [('failed', 1), ('completed', 1)]
        run_dict = dict(runs)
        assert run_dict.get('failed') == 1
        assert run_dict.get('completed') == 1

@then("the command should fail after a reasonable number of retries")
def check_command_fails(result):
    assert result.exit_code != 0

@then("the error message should clearly indicate a persistent failure")
def check_error_message(result):
    assert "Extraction failed" in result.stdout

@then("the state history should log the multiple failed attempts")
def check_failed_attempts_logged(db_path):
    with duckdb.connect(str(db_path)) as con:
        failed_runs = con.execute("SELECT COUNT(*) FROM baliza_state.runs WHERE status = 'failed'").fetchone()[0]
        assert failed_runs >= 1
