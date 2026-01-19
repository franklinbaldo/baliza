"""BDD step definitions for resilience.feature (Tier 1: Core Features)."""

from pathlib import Path

import duckdb
import pytest
from pytest_bdd import given, when, then, scenario, parsers
from typer.testing import CliRunner

from baliza.cli import app

runner = CliRunner()

# Mark all resilience scenarios as Tier 1 (Core Features)
pytestmark = pytest.mark.tier1


# =============================================================================
# Scenario: Handles PNCP API errors gracefully
# =============================================================================


@pytest.mark.skip(reason="Requires HTTP mocking setup")
@scenario('../features/resilience.feature', 'Handles PNCP API errors gracefully')
def test_handles_api_errors_gracefully():
    pass


@given("the PNCP API returns a 500 error", target_fixture="mock_api_error")
def mock_api_error(httpx_mock):
    """Mock the PNCP API to return a 500 error."""
    # TODO: Configure httpx_mock to return 500 for PNCP requests
    # This requires understanding the exact URLs baliza calls
    return httpx_mock


@when(parsers.parse('I run "baliza extract --start {start} --end {end}"'), target_fixture="error_result")
def run_extract_with_error(tmp_path, mock_api_error, start, end):
    """Run baliza extract when API is returning errors."""
    db_path = tmp_path / "test.duckdb"
    result = runner.invoke(
        app,
        [
            "extract",
            "--duckdb",
            str(db_path),
            "--start-date",
            start,
            "--end-date",
            end,
            "--dataset",
            "test_dataset",
        ],
    )
    return {"result": result, "db_path": db_path}


@then("the command should fail with exit code 1")
def check_exit_code_1(error_result):
    """Verify command failed with exit code 1."""
    assert error_result["result"].exit_code == 1, f"Expected exit code 1, got {error_result['result'].exit_code}"


@then("the error message should be clear")
def check_clear_error(error_result):
    """Verify error message is human-readable."""
    output = error_result["result"].stdout + error_result["result"].stderr
    # Check that error message contains something useful
    assert len(output) > 0, "No error message displayed"
    # TODO: Add more specific checks for error message quality


@then("no partial data should be saved")
def check_no_partial_data(error_result):
    """Verify no data was saved when extraction failed."""
    db_path = error_result["db_path"]

    # If database doesn't exist, that's fine
    if not db_path.exists():
        return

    # If database exists, check that contratos table is empty or doesn't exist
    with duckdb.connect(str(db_path), read_only=True) as con:
        try:
            count = con.execute("SELECT COUNT(*) FROM test_dataset.contratos").fetchone()[0]
            assert count == 0, f"Found {count} rows when expecting 0 (no partial data)"
        except duckdb.CatalogException:
            # Table doesn't exist, which is fine
            pass
