"""Step definitions for the backfill feature."""

import pytest
from pytest_bdd import given, scenario, then, when
from typer.testing import CliRunner

from baliza.cli_simple import app

runner = CliRunner()

# All backfill scenarios are Tier 1 (Core Features)
pytestmark = pytest.mark.tier1


@pytest.mark.skip(reason="Backfill command not yet implemented")
@scenario("../features/backfill.feature", "Backfill a single month")
def test_backfill_single_month():
    """Backfill a single month."""


@pytest.mark.skip(reason="Backfill command not yet implemented")
@scenario("../features/backfill.feature", "Backfill multiple months sequentially")
def test_backfill_multiple_months():
    """Backfill multiple months sequentially."""


@pytest.mark.skip(reason="Backfill command not yet implemented")
@scenario("../features/backfill.feature", "Invalid month format")
def test_backfill_invalid_format():
    """Invalid month format."""


@pytest.mark.skip(reason="Backfill command not yet implemented")
@scenario("../features/backfill.feature", "Backfill updates coverage manifesto")
def test_backfill_updates_manifesto():
    """Backfill updates coverage manifesto."""


# --- Givens ---


@given("a clean local data store")
def clean_data_store(tmp_path):
    """Ensure a clean DuckDB database for the test."""
    db_path = tmp_path / "test_backfill.duckdb"
    if db_path.exists():
        db_path.unlink()
    return db_path


# --- Whens ---


@when(
    'I run the backfill command for "{start_month}" to "{end_month}"',
    target_fixture="result",
)
def run_backfill(clean_data_store, start_month, end_month):
    """Run the backfill command."""
    # This will fail as 'backfill' is not in app
    result = runner.invoke(
        app,
        ["backfill", "--start", start_month, "--end", end_month, "--duckdb", str(clean_data_store)],
    )
    return result


@when('I run the backfill command with an invalid month "{month}"', target_fixture="result")
def run_backfill_invalid(clean_data_store, month):
    """Run the backfill command with invalid input."""
    result = runner.invoke(
        app, ["backfill", "--start", month, "--end", month, "--duckdb", str(clean_data_store)]
    )
    return result


# --- Thens ---


@then("the system should extract data for the entire month of January 2024")
def check_january_extracted(result):
    """Verify January 2024 data was extracted."""
    # Assertions would go here if command was implemented
    assert result.exit_code == 0


@then('the coverage state should reflect that "2024-01" is complete')
def check_january_coverage(result):
    """Verify coverage state."""
    assert "2024-01" in result.stdout


@then("the system should process November 2023, December 2023, and January 2024")
def check_multiple_months_processed(result):
    """Verify multiple months were processed."""
    assert result.exit_code == 0


@then("the data for all three months should be present in the data store")
def check_data_present(result):
    """Verify data presence."""
    assert "November 2023" in result.stdout
    assert "December 2023" in result.stdout
    assert "January 2024" in result.stdout


@then("the command should fail with a validation error")
def check_validation_error(result):
    """Verify failure on invalid input."""
    assert result.exit_code != 0


@then('it should explain that the format must be YYYY-MM')
def check_error_message(result):
    """Verify error message."""
    assert "YYYY-MM" in result.stdout


@then("a run record should be created in the history")
def check_run_record(result):
    """Verify run record creation."""
    assert result.exit_code == 0


@then("the coverage table should have entries for February 2024")
def check_february_entries(result):
    """Verify coverage entries."""
    assert "2024-02" in result.stdout
