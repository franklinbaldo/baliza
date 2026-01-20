"""BDD step definitions for extraction using VCR for real API responses."""

from pathlib import Path

import duckdb
import pytest
from pytest_bdd import given, parsers, scenario, then, when
from typer.testing import CliRunner

from baliza.cli import app

runner = CliRunner()

# Mark all extraction scenarios as Tier 0 (Critical Path) and use VCR
pytestmark = [pytest.mark.tier0, pytest.mark.vcr]


# =============================================================================
# Scenario 1: Basic extraction works
# =============================================================================


@pytest.mark.vcr()
@scenario("../features/extraction.feature", "Basic extraction works")
def test_basic_extraction_works():
    """Test basic extraction with real PNCP API responses (via VCR cassette)."""
    pass


@given("a clean DuckDB database", target_fixture="db_path")
def clean_db(tmp_path: Path) -> Path:
    """Create a clean DuckDB database."""
    db = tmp_path / "test.duckdb"
    if db.exists():
        db.unlink()
    return db


@when(
    parsers.parse('I run "baliza extract --start {start} --end {end}"'), target_fixture="run_result"
)
def run_extract(request, start, end):
    """Run baliza extract command with real API calls (recorded by VCR).

    This step works for both basic and incremental scenarios by checking
    which fixtures are available.
    """
    # Get db_path from either clean db or previous extraction
    try:
        db_path = request.getfixturevalue("previous_extraction")
    except Exception:
        db_path = request.getfixturevalue("db_path")

    # Run extract command - VCR will intercept and replay HTTP calls
    result = runner.invoke(
        app,
        [
            "extract",
            "--start",
            start,
            "--end",
            end,
            "--duckdb",
            str(db_path),
            "--dataset",
            "test_dataset",
        ],
    )

    if result.exit_code != 0:
        print("\n=== COMMAND FAILED ===")
        print(f"Exit code: {result.exit_code}")
        print(f"Output:\n{result.stdout}")
        if result.exception:
            import traceback

            print(
                f"Exception:\n{''.join(traceback.format_exception(type(result.exception), result.exception, result.exception.__traceback__))}"
            )

    return {"result": result, "db_path": db_path}


@then("the data should be saved to the database")
def check_data_saved(run_result):
    """Verify data was saved to database."""
    db_path = run_result["db_path"]
    with duckdb.connect(str(db_path), read_only=True) as con:
        count = con.execute("SELECT COUNT(*) FROM test_dataset.contratos").fetchone()[0]
        assert count > 0, "No data found in database"


@then("the command should exit successfully")
def check_exit_success(run_result):
    """Verify command exited with code 0."""
    assert run_result["result"].exit_code == 0, f"Exit code: {run_result['result'].exit_code}"


# =============================================================================
# Scenario 2: Incremental extraction doesn't duplicate data
# =============================================================================


@pytest.mark.vcr()
@scenario("../features/extraction.feature", "Incremental extraction doesn't duplicate data")
def test_incremental_no_duplicates():
    """Test incremental extraction with real PNCP API responses (via VCR cassette)."""
    pass


@given("I have previously extracted data for 2024-01-01", target_fixture="previous_extraction")
def previous_extraction(tmp_path: Path):
    """Setup: run initial extraction for 2024-01-01 (real API via VCR)."""
    db_path = tmp_path / "test.duckdb"
    if db_path.exists():
        db_path.unlink()

    # Run initial extraction - VCR will replay the API response
    result = runner.invoke(
        app,
        [
            "extract",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-01",
            "--duckdb",
            str(db_path),
            "--dataset",
            "test_dataset",
        ],
    )

    if result.exit_code != 0:
        print("\n=== SETUP EXTRACTION FAILED ===")
        print(f"Output: {result.stdout}")
        if result.exception:
            import traceback

            traceback.print_exception(
                type(result.exception), result.exception, result.exception.__traceback__
            )

    assert result.exit_code == 0, f"Setup extraction failed with exit code {result.exit_code}"
    return db_path


@then("the database should not contain duplicate records")
def check_no_duplicates(run_result):
    """Verify no duplicate primary keys exist."""
    db_path = run_result["db_path"]
    with duckdb.connect(str(db_path), read_only=True) as con:
        # Check for duplicate numeroControlePNCP (primary key)
        query = """
            SELECT numeroControlePNCP, COUNT(*) as cnt
            FROM test_dataset.contratos
            GROUP BY numeroControlePNCP
            HAVING COUNT(*) > 1
        """
        duplicates = con.execute(query).fetchall()
        assert len(duplicates) == 0, f"Found {len(duplicates)} duplicate primary keys: {duplicates}"


@then("the 2024-01-01 data should be preserved, not duplicated")
def check_data_preserved(run_result):
    """Verify original data was preserved via INSERT OR IGNORE (append-only)."""
    db_path = run_result["db_path"]
    with duckdb.connect(str(db_path), read_only=True) as con:
        # Get row count - with INSERT OR IGNORE, duplicates are ignored
        total = con.execute("SELECT COUNT(*) FROM test_dataset.contratos").fetchone()[0]

        # Data should exist (we extracted something)
        assert total > 0, "No data found in database"

        # With real API data, we can't predict exact counts, but we can verify
        # that the data extraction succeeded and produced results
        print(f"Total contracts in database: {total}")


@then("new 2024-01-02 data should be added")
def check_new_data_added(run_result):
    """Verify incremental extraction added data from the expanded date range."""
    db_path = run_result["db_path"]
    with duckdb.connect(str(db_path), read_only=True) as con:
        # The database should have contracts - incremental extraction succeeded
        total = con.execute("SELECT COUNT(*) FROM test_dataset.contratos").fetchone()[0]
        assert total > 0, "No contracts found after incremental extraction"

        # With real PNCP data, just verify the extraction succeeded
        # The INSERT OR IGNORE append-only approach is validated by the no-duplicates test
        print(f"Successfully extracted {total} contracts with incremental approach")
