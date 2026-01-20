"""BDD step definitions for resilience.feature (Tier 1: Core Features)."""

from pathlib import Path
from unittest.mock import Mock

import duckdb
import httpx
import pytest
from pytest_bdd import given, parsers, scenario, then, when
from typer.testing import CliRunner

from baliza.cli import app

runner = CliRunner()

# Mark all resilience scenarios as Tier 1 (Core Features)
pytestmark = pytest.mark.tier1


# =============================================================================
# Scenario: Handles PNCP API errors gracefully
# =============================================================================


@scenario("../features/resilience.feature", "Handles PNCP API errors gracefully")
def test_handles_api_errors_gracefully():
    pass


@given("the PNCP API returns a 500 error", target_fixture="api_error_setup")
def api_error_setup(tmp_path: Path):
    """Setup: mock API to return 500 error."""
    db_path = tmp_path / "test.duckdb"
    if db_path.exists():
        db_path.unlink()
    return db_path


@when(
    parsers.parse('I run "baliza extract --start {start} --end {end}"'),
    target_fixture="error_result",
)
def run_extract_with_error(api_error_setup, monkeypatch, start, end):
    """Run baliza extract when API is returning errors."""

    # Mock HTTP to return 500 error
    def mock_get(*args, **kwargs):
        response = Mock()
        response.status_code = 500
        response.text = "Internal Server Error"

        def raise_for_status():
            raise httpx.HTTPStatusError("500 Server Error", request=Mock(), response=response)

        response.raise_for_status = raise_for_status
        return response

    monkeypatch.setattr("httpx.Client.get", mock_get)

    result = runner.invoke(
        app,
        [
            "extract",
            "--start",
            start,
            "--end",
            end,
            "--duckdb",
            str(api_error_setup),
            "--dataset",
            "test_dataset",
        ],
    )
    return {"result": result, "db_path": api_error_setup}


@then("the command should fail with exit code 1")
def check_exit_code_1(error_result):
    """Verify command failed with exit code 1."""
    assert error_result["result"].exit_code == 1, (
        f"Expected exit code 1, got {error_result['result'].exit_code}"
    )


@then("the error message should be clear")
def check_clear_error(error_result):
    """Verify error message is human-readable."""
    output = error_result["result"].stdout
    # Check that error message contains something useful
    assert len(output) > 0, "No error message displayed"
    assert "✗" in output or "fail" in output.lower() or "error" in output.lower(), (
        "No clear error indicator"
    )


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
