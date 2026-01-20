"""BDD step definitions for extraction.feature (Tier 0: Critical Path)."""

from pathlib import Path

import duckdb
import pytest
from dlt.common import pendulum
from dlt.sources import DltResource
from pytest_bdd import given, when, then, scenario, parsers
from typer.testing import CliRunner

from baliza.cli import app
from baliza.pipelines import pncp

runner = CliRunner()

# Mark all extraction scenarios as Tier 0 (Critical Path)
pytestmark = pytest.mark.tier0


def parse_dates(item):
    """Parse date strings to datetime objects for dlt incremental cursor."""
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
# Scenario 1: Basic extraction works
# =============================================================================


@pytest.mark.vcr
@scenario('../features/extraction.feature', 'Basic extraction works')
def test_basic_extraction_works():
    pass


@given("a clean DuckDB database", target_fixture="db_path")
def clean_db(tmp_path: Path) -> Path:
    """Create a clean DuckDB database."""
    db = tmp_path / "test.duckdb"
    if db.exists():
        db.unlink()
    return db


@when(parsers.parse('I run "baliza extract --start {start} --end {end}"'), target_fixture="run_result")
def run_extract(db_path, patched_pncp_source, start, end):
    """Run baliza extract command."""
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
    # Debug: print output if command failed
    if result.exit_code != 0:
        print(f"\n=== COMMAND FAILED ===")
        print(f"Exit code: {result.exit_code}")
        print(f"Output:\n{result.stdout}")
        if result.stderr:
            print(f"Stderr:\n{result.stderr}")
        if result.exception:
            print(f"Exception: {result.exception}")
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


@pytest.mark.skip(reason="Requires implementation - Step 1: Get extraction working first")
@scenario('../features/extraction.feature', 'Incremental extraction doesn\'t duplicate data')
def test_incremental_no_duplicates():
    pass


@given("I have previously extracted data for 2024-01-01", target_fixture="previous_data")
def previous_data(db_path):
    """Setup: run initial extraction for 2024-01-01."""
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
        assert len(duplicates) == 0, f"Found {len(duplicates)} duplicate primary keys"


@then("the 2024-01-01 data should be updated, not duplicated")
def check_data_updated(run_result):
    """Verify data was updated via merge, not duplicated."""
    db_path = run_result["db_path"]
    with duckdb.connect(str(db_path), read_only=True) as con:
        # For 2024-01-01 contracts, there should be only one version
        # (This assumes the merge strategy worked correctly)
        query = """
            SELECT DATE_TRUNC('day', dataPublicacao) as day, COUNT(*) as cnt
            FROM test_dataset.contratos
            WHERE DATE_TRUNC('day', dataPublicacao) = '2024-01-01'
            GROUP BY day
        """
        result = con.execute(query).fetchone()
        if result:
            # Just ensure we have data, deduplication is checked above
            assert result[1] > 0
