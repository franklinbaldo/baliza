"""BDD step definitions for backfill.feature (Tier 1: Core Features)."""

from pathlib import Path

import duckdb
import pytest
from pytest_bdd import given, when, then, scenario, parsers
from typer.testing import CliRunner

from baliza.cli import app

runner = CliRunner()

# Mark all backfill scenarios as Tier 1 (Core Features)
pytestmark = pytest.mark.tier1


# =============================================================================
# Scenario 1: Backfill a single month
# =============================================================================


@pytest.mark.skip(reason="Requires backfill command implementation")
@scenario('../features/backfill.feature', 'Backfill a single month')
def test_backfill_single_month():
    pass


@given("I want to backfill January 2024", target_fixture="backfill_target")
def backfill_january_2024(tmp_path):
    db_path = tmp_path / "test.duckdb"
    return {"db_path": db_path, "start_month": "2024-01", "end_month": "2024-01"}


@when(parsers.parse('I run "baliza backfill {start_month} {end_month}"'), target_fixture="backfill_result")
def run_backfill_command(backfill_target, start_month, end_month):
    result = runner.invoke(
        app,
        [
            "backfill",
            start_month,
            end_month,
            "--duckdb",
            str(backfill_target["db_path"]),
            "--dataset",
            "test_dataset",
        ],
    )
    return {"result": result, "db_path": backfill_target["db_path"]}


@then("the command should process the full month deterministically")
def check_full_month_processed(backfill_result):
    assert backfill_result["result"].exit_code == 0
    # TODO: Verify all days in January 2024 were processed


@then("the backfill should use daily windows")
def check_daily_windows(backfill_result):
    # TODO: Verify windows are 1-day windows, not hourly
    pass


@then("state should record backfill as a separate pipeline")
def check_backfill_pipeline(backfill_result):
    db_path = backfill_result["db_path"]
    with duckdb.connect(str(db_path), read_only=True) as con:
        # TODO: Check that pipeline_name is "pncp_backfill" not "pncp_incremental"
        pass


# =============================================================================
# Shared steps (reusable across multiple scenarios)
# =============================================================================


@given("a clean DuckDB database", target_fixture="clean_db")
def clean_db(tmp_path: Path) -> Path:
    """Create a clean DuckDB database."""
    db_path = tmp_path / "test.duckdb"
    if db_path.exists():
        db_path.unlink()
    return db_path


@then("the extraction should complete without errors")
def check_no_errors(backfill_result):
    assert backfill_result["result"].exit_code == 0
    assert "error" not in backfill_result["result"].stdout.lower() or "0 errors" in backfill_result["result"].stdout.lower()


@then("data should be stored in the DuckDB database")
def check_data_stored(backfill_result):
    db_path = backfill_result["db_path"]
    with duckdb.connect(str(db_path), read_only=True) as con:
        # Check that data exists (table should have rows)
        result = con.execute("SELECT COUNT(*) FROM test_dataset.contratos").fetchone()
        if result:
            assert result[0] > 0, "No data found in database"
