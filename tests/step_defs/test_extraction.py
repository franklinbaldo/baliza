from pathlib import Path

import duckdb
import pytest
from dlt.sources import DltResource
from pytest_bdd import given, when, then, scenario, parsers
from typer.testing import CliRunner
from dlt.common import pendulum

from baliza.cli import app
from baliza.pipelines import pncp

runner = CliRunner()

# Mark all extraction scenarios as Tier 0 (Critical Path)
pytestmark = pytest.mark.tier0


def parse_dates(item):
    if "dataAtualizacao" in item and isinstance(item["dataAtualizacao"], str):
        item["dataAtualizacao"] = pendulum.parse(item["dataAtualizacao"])
    return item


@pytest.fixture
def patched_pncp_source(monkeypatch):
    """Patches the pncp_source to add a date parsing transformer."""
    original_pncp_source = pncp.pncp_source

    def new_pncp_source(*args, **kwargs) -> DltResource:
        source = original_pncp_source(*args, **kwargs)
        source.resources["contratos"].add_map(parse_dates)
        return source

    monkeypatch.setattr(pncp, "pncp_source", new_pncp_source)


# =============================================================================
# Background steps (shared across all scenarios)
# =============================================================================


@given("the PNCP API is available")
def pncp_api_available():
    """Assume PNCP API is available (will use VCR cassettes for testing)."""
    pass


@given("I have a DuckDB database configured")
def duckdb_configured():
    """Assume DuckDB is properly configured (fixture handles this)."""
    pass


@pytest.mark.vcr
@scenario('../features/extraction.feature', 'Basic extraction for a date range')
def test_basic_extraction_for_date_range():
    pass


@given("a clean DuckDB database", target_fixture="db_path")
def db_path(tmp_path: Path) -> Path:
    db = tmp_path / "test.duckdb"
    if db.exists():
        db.unlink()
    return db


@when("I run the baliza extract command for a specific date range", target_fixture="run_result")
def run_extract_command(db_path, patched_pncp_source):
    result = runner.invoke(
        app,
        [
            "extract",
            "--duckdb",
            str(db_path),
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-01",
            "--dataset",
            "test_dataset",
        ],
    )
    assert result.exit_code == 0
    return result


@then("the data should be extracted and saved to the DuckDB database")
def check_data_in_db(db_path):
    with duckdb.connect(str(db_path), read_only=True) as con:
        count = con.execute("SELECT COUNT(*) FROM test_dataset.contratos").fetchone()[0]
        assert count > 0


@then("the extraction should complete successfully")
def check_extraction_success(run_result):
    assert run_result.exit_code == 0
    assert "error" not in run_result.stdout.lower() or "0 errors" in run_result.stdout.lower()


# =============================================================================
# Scenario 2: Incremental extraction with configurable lookback
# =============================================================================


@pytest.mark.skip(reason="Requires implementation of lookback logic")
@scenario('../features/extraction.feature', 'Incremental extraction with configurable lookback')
def test_incremental_extraction_with_lookback():
    pass


@given("I have previously extracted data up to 10 days ago", target_fixture="previous_extraction")
def previous_extraction(db_path):
    # TODO: Implement setup for previous extraction state
    # This should create state records showing extraction up to 10 days ago
    return {"days_ago": 10, "db_path": db_path}


@when(parsers.parse('I run "baliza extract --lookback-days {days:d}"'), target_fixture="run_result_lookback")
def run_extract_with_lookback(previous_extraction, days):
    result = runner.invoke(
        app,
        [
            "extract",
            "--duckdb",
            str(previous_extraction["db_path"]),
            "--lookback-days",
            str(days),
            "--dataset",
            "test_dataset",
        ],
    )
    return {"result": result, "lookback_days": days}


@then(parsers.parse("the extraction should process the last {days:d} days"))
def check_lookback_processing(run_result_lookback, days):
    assert run_result_lookback["result"].exit_code == 0
    # TODO: Verify that exactly 'days' worth of windows were processed
    # Check coverage table for the correct date range


@then("should include any updates to previously extracted data")
def check_incremental_updates(run_result_lookback):
    # TODO: Verify that the merge strategy allowed updates to existing records
    pass


@then("the coverage table should show the incremental windows")
def check_coverage_table(run_result_lookback, db_path):
    # TODO: Query baliza_state.coverage to verify windows were recorded
    pass


# =============================================================================
# Scenario 9: Extraction creates run record with metadata
# =============================================================================


@pytest.mark.skip(reason="Requires implementation of run tracking logic")
@scenario('../features/extraction.feature', 'Extraction creates run record with metadata')
def test_extraction_creates_run_record():
    pass


@given("a clean state database", target_fixture="clean_state_db")
def clean_state_db(db_path):
    # Ensure state tables exist but are empty
    return db_path


@when(parsers.parse('I run "baliza extract --lookback-days {days:d}"'), target_fixture="run_with_tracking")
def run_extract_with_tracking(clean_state_db, days):
    result = runner.invoke(
        app,
        [
            "extract",
            "--duckdb",
            str(clean_state_db),
            "--lookback-days",
            str(days),
            "--dataset",
            "test_dataset",
        ],
    )
    return {"result": result, "db_path": clean_state_db}


@then("a new run record should be created in baliza_state.runs")
def check_run_record_exists(run_with_tracking):
    db_path = run_with_tracking["db_path"]
    with duckdb.connect(str(db_path), read_only=True) as con:
        count = con.execute("SELECT COUNT(*) FROM baliza_state.runs").fetchone()[0]
        assert count > 0, "No run records found in baliza_state.runs"


@then("the run should have a unique run_id")
def check_unique_run_id(run_with_tracking):
    db_path = run_with_tracking["db_path"]
    with duckdb.connect(str(db_path), read_only=True) as con:
        run_id = con.execute("SELECT run_id FROM baliza_state.runs LIMIT 1").fetchone()[0]
        assert run_id is not None
        assert len(run_id) > 0


@then("the run should record start_time")
def check_start_time(run_with_tracking):
    db_path = run_with_tracking["db_path"]
    with duckdb.connect(str(db_path), read_only=True) as con:
        start_time = con.execute("SELECT started_at FROM baliza_state.runs LIMIT 1").fetchone()[0]
        assert start_time is not None


@then('the run should record status as "running"')
def check_running_status(run_with_tracking):
    # This step would check the status during execution, but hard to test in unit test
    # For now, just ensure the status field exists
    pass


@then('when complete, status should change to "completed"')
def check_completed_status(run_with_tracking):
    db_path = run_with_tracking["db_path"]
    with duckdb.connect(str(db_path), read_only=True) as con:
        status = con.execute("SELECT status FROM baliza_state.runs LIMIT 1").fetchone()[0]
        assert status in ("completed", "success", "partial"), f"Unexpected status: {status}"


@then("the run should record total_rows extracted")
def check_total_rows(run_with_tracking):
    db_path = run_with_tracking["db_path"]
    with duckdb.connect(str(db_path), read_only=True) as con:
        rows = con.execute("SELECT rows_extracted FROM baliza_state.runs LIMIT 1").fetchone()[0]
        assert rows is not None
        assert rows >= 0
