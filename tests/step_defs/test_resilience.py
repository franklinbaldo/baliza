"""BDD step definitions for resilience.feature."""

import uuid
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

# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def api_mock_state():
    return {
        "failure_count": 0,
        "total_calls": 0,
        "behavior": "normal" # or "transient", "persistent"
    }

@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Create a clean DuckDB database."""
    db = tmp_path / "test_resilience.duckdb"
    if db.exists():
        db.unlink()
    return db

# =============================================================================
# Background Steps
# =============================================================================

@given("a clean local data store", target_fixture="db_path")
def given_clean_db(tmp_path: Path) -> Path:
    db = tmp_path / "resilience.duckdb"
    if db.exists():
        db.unlink()
    return db

@given("a PNCP API that will fail transiently", target_fixture="api_mock_state")
def given_api_fail_transiently(monkeypatch, api_mock_state):
    def mock_get(self, url, **kwargs):
        api_mock_state["total_calls"] += 1
        params = kwargs.get("params", {})
        req = httpx.Request("GET", url, params=params)

        if api_mock_state["behavior"] == "transient":
            # Fail for the first page of the first half
            if params.get("dataInicial") == "20240101" and api_mock_state["failure_count"] < 5:
                api_mock_state["failure_count"] += 1
                return httpx.Response(500, content="Internal Server Error", request=req)

        if api_mock_state["behavior"] == "persistent":
            api_mock_state["failure_count"] += 1
            return httpx.Response(500, content="Internal Server Error", request=req)

        # Success path
        record = {
            "numeroControlePNCP": f"PNCP-{uuid.uuid4()}",
            "dataPublicacao": "2024-01-01T10:00:00"
        }
        return httpx.Response(200, json={"data": [record], "totalPaginas": 1}, request=req)

    monkeypatch.setattr(httpx.Client, "get", mock_get)
    return api_mock_state

# =============================================================================
# Scenario: The extract command recovers from a transient API error
# =============================================================================

@scenario(
    "../features/resilience.feature",
    "The extract command recovers from a transient API error",
)
def test_extract_recovers_from_transient_error():
    """Scenario: The extract command recovers from a transient API error."""

@given("the PNCP API will return a 500 error for the first half of a date range")
def set_transient_behavior(api_mock_state):
    api_mock_state["behavior"] = "transient"

@given("the PNCP API will succeed for the second half of the date range")
def set_success_second_half():
    # Already handled by the mock logic
    pass

@when('I run the "baliza extract" command for the full date range', target_fixture="extraction_result")
def run_extract_full_range(db_path, api_mock_state):
    # First attempt: should fail because failure_count < 5 and tenacity only retries 4 times (total 4 attempts)
    result = runner.invoke(
        app,
        [
            "extract",
            "--start", "2024-01-01",
            "--end", "2024-01-02",
            "--duckdb", str(db_path),
        ],
    )
    # The first command MUST fail for this scenario.
    assert result.exit_code != 0

    # Second attempt: should succeed because failure_count is already 4.
    # Next call will fail (failure_count becomes 5), then next call (retry) will succeed.
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
    return result2

@then("the command should eventually succeed")
def check_eventual_success(extraction_result):
    assert extraction_result.exit_code == 0

@then("the final dataset should contain all records for the full date range")
def check_final_data(db_path):
    with duckdb.connect(str(db_path)) as con:
        count = con.execute("SELECT COUNT(*) FROM baliza_raw.contratos").fetchone()[0]
        assert count > 0

@then("the final dataset should not contain duplicate records")
def check_no_duplicates(db_path):
    with duckdb.connect(str(db_path)) as con:
        res = con.execute("SELECT numeroControlePNCP, COUNT(*) FROM baliza_raw.contratos GROUP BY 1 HAVING COUNT(*) > 1").fetchall()
        assert len(res) == 0

@then("the run history should show one failed run and one successful run")
def check_run_history(db_path):
    with duckdb.connect(str(db_path)) as con:
        # Check if table exists
        exists = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'runs' AND table_schema = 'baliza_state'").fetchone()[0]
        assert exists > 0

        # Check history
        runs = con.execute("SELECT status FROM baliza_state.runs ORDER BY started_at").fetchall()
        assert len(runs) >= 2
        assert runs[0][0] == "failed"
        assert runs[1][0] == "completed"

# =============================================================================
# Scenario: The extract command gives up after multiple consecutive failures
# =============================================================================

@scenario(
    "../features/resilience.feature",
    "The extract command gives up after multiple consecutive failures",
)
def test_extract_fails_after_multiple_retries():
    """Scenario: The extract command gives up after multiple consecutive failures."""

@given("the PNCP API will consistently return a 500 error")
def set_persistent_behavior(api_mock_state):
    api_mock_state["behavior"] = "persistent"

@when('I run the "baliza extract" command')
def run_extract_persistent_fail(db_path):
    result = runner.invoke(
        app,
        [
            "extract",
            "--start", "2024-01-01",
            "--end", "2024-01-01",
            "--duckdb", str(db_path),
        ],
    )
    pytest.result = result

@then("the command should fail after a reasonable number of retries")
def check_fail_after_retries():
    assert pytest.result.exit_code != 0

@then("the error message should clearly indicate a persistent failure")
def check_error_message():
    assert "Extraction failed" in pytest.result.stdout

@then("the state history should log the multiple failed attempts")
def check_state_history_fail(db_path):
    with duckdb.connect(str(db_path)) as con:
        runs = con.execute("SELECT status FROM baliza_state.runs WHERE status = 'failed'").fetchall()
        assert len(runs) > 0
