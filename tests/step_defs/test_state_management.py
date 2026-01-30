"""Step definitions for the state management feature."""

from pathlib import Path

import duckdb
from pytest_bdd import given, scenario, then, when
from typer.testing import CliRunner

from baliza.cli_simple import app

runner = CliRunner()


@scenario(
    "../features/state_management.feature",
    "Show the overall status of the data extraction process",
)
def test_show_status():
    """Show the overall status of the data extraction process."""


@scenario(
    "../features/state_management.feature",
    "Verify the data coverage and find gaps",
)
def test_verify_gaps():
    """Verify the data coverage and find gaps."""


@scenario(
    "../features/state_management.feature",
    "Show warnings for pending extractions",
)
def test_status_warnings():
    """Show warnings for pending extractions."""


# --- Givens ---


@given("a data store with extracted records", target_fixture="populated_db")
def populated_db(db_path: Path) -> Path:
    """Create a DuckDB database with some records."""
    with duckdb.connect(str(db_path)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS baliza_raw")
        con.execute("CREATE TABLE baliza_raw.contratos (dataPublicacao TIMESTAMP)")
        con.execute("INSERT INTO baliza_raw.contratos VALUES ('2024-01-01 10:00:00'), ('2024-01-02 11:00:00')")
    return db_path


@given("a data store with partial coverage", target_fixture="partial_db")
def partial_db(db_path: Path) -> Path:
    """Create a DuckDB database with partial coverage."""
    with duckdb.connect(str(db_path)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS baliza_state")
        con.execute("""
            CREATE TABLE baliza_state.coverage (
                resource VARCHAR,
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                status VARCHAR,
                total_paginas INTEGER,
                rows_extracted INTEGER,
                extracted_at TIMESTAMP
            )
        """)
        # Window for Jan 1st
        con.execute("INSERT INTO baliza_state.coverage VALUES ('contratos', '2024-01-01', '2024-01-01 23:59:59', 'complete', 1, 10, NOW())")
        # Gap on Jan 2nd
        # Window for Jan 3rd
        con.execute("INSERT INTO baliza_state.coverage VALUES ('contratos', '2024-01-03', '2024-01-03 23:59:59', 'complete', 1, 10, NOW())")
    return db_path


@given("a data store with pending checkpoints", target_fixture="checkpoint_db")
def checkpoint_db(db_path: Path) -> Path:
    """Create a DuckDB database with pending checkpoints."""
    with duckdb.connect(str(db_path)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS baliza_state")
        con.execute("""
            CREATE TABLE baliza_state.extraction_checkpoint (
                resource VARCHAR,
                extraction_date DATE,
                current_page INTEGER,
                total_pages INTEGER,
                rows_extracted INTEGER,
                started_at TIMESTAMP,
                updated_at TIMESTAMP
            )
        """)
        con.execute("INSERT INTO baliza_state.extraction_checkpoint (resource, extraction_date, current_page, total_pages, rows_extracted, started_at, updated_at) VALUES ('contratos', '2024-01-04', 5, 10, 500, NOW(), NOW())")

        # Also need contracts table for status to work
        con.execute("CREATE SCHEMA IF NOT EXISTS baliza_raw")
        con.execute("CREATE TABLE baliza_raw.contratos (dataPublicacao TIMESTAMP)")
        con.execute("INSERT INTO baliza_raw.contratos VALUES ('2024-01-01 10:00:00')")
    return db_path


# --- Whens ---


@when('I run the "baliza status" command', target_fixture="result")
def run_status(db_path):
    """Run the 'status' command."""
    return runner.invoke(app, ["status", "--duckdb", str(db_path)])


@when('I run the "baliza verify" command for the full range', target_fixture="result")
def run_verify(partial_db):
    """Run the 'verify' command."""
    return runner.invoke(app, [
        "verify",
        "--resource", "contratos",
        "--start", "2024-01-01",
        "--end", "2024-01-03",
        "--duckdb", str(partial_db)
    ])


# --- Thens ---


@then("the output should summarize the state of the data store")
def check_status_summary(result):
    """Check that the output summarizes the state of the data store."""
    assert result.exit_code == 0
    assert "Baliza PNCP Status" in result.stdout
    assert "Total contracts" in result.stdout
    assert "2" in result.stdout


@then("the output should list the missing windows")
def check_missing_windows(result):
    """Check that the output lists the missing windows."""
    assert result.exit_code == 0
    assert "Found 1 gap(s)" in result.stdout
    assert "2024-01-02" in result.stdout


@then("the output should show a warning about incomplete extractions")
def check_status_warning(result):
    """Check that the output shows a warning about incomplete extractions."""
    assert result.exit_code == 0
    assert "1 extraction(s) incomplete" in result.stdout
