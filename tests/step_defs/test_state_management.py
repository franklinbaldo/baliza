"""Step definitions for the state management feature."""

from pathlib import Path
from textwrap import dedent

import duckdb
import pytest
from pytest_bdd import given, scenario, then, when
from typer.testing import CliRunner

from baliza.cli_simple import app

runner = CliRunner()

# Mark all scenarios in this file with tier1
pytestmark = pytest.mark.tier1


@scenario(
    "../features/state_management.feature",
    "Show the state of the data extraction process",
)
def test_show_state():
    """Show the state of the data extraction process."""


@scenario(
    "../features/state_management.feature",
    "List the gaps in the data extraction process",
)
def test_list_gaps():
    """List the gaps in the data extraction process."""


@scenario(
    "../features/state_management.feature",
    "Show the history of the data extraction process",
)
def test_show_history():
    """Show the history of the data extraction process."""


# --- Givens ---


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Create a clean DuckDB database."""
    db = tmp_path / "test_state.duckdb"
    if db.exists():
        db.unlink()
    return db


@given(
    "a data store with complete, incomplete, and missing windows",
    target_fixture="db_path_with_windows",
)
def db_path_with_windows(db_path: Path) -> Path:
    """Create a DuckDB database with a mix of window states."""
    with duckdb.connect(str(db_path)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS baliza_state")
        con.execute(
            dedent(
                """
            CREATE TABLE IF NOT EXISTS baliza_state.coverage (
                resource VARCHAR,
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                status VARCHAR,
                total_paginas INTEGER,
                rows_extracted INTEGER,
                extracted_at TIMESTAMP
            )
        """
            )
        )
        # Complete window for 2024-01-01
        con.execute(
            "INSERT INTO baliza_state.coverage VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                "contratos",
                "2024-01-01 00:00:00",
                "2024-01-01 23:59:59",
                "complete",
                1,
                100,
                "2024-01-01 10:00:00"
            ],
        )
        # Incomplete window for 2024-01-03
        con.execute(
            "INSERT INTO baliza_state.coverage VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                "contratos",
                "2024-01-03 00:00:00",
                "2024-01-03 23:59:59",
                "incomplete",
                1,
                50,
                "2024-01-03 10:00:00"
            ],
        )

        # Create raw table so 'status' command doesn't fail
        con.execute("CREATE SCHEMA IF NOT EXISTS baliza_raw")
        con.execute("CREATE TABLE IF NOT EXISTS baliza_raw.contratos (dataPublicacao TIMESTAMP)")
        con.execute("INSERT INTO baliza_raw.contratos VALUES ('2024-01-01 12:00:00')")

    return db_path


@given(
    "a data store with a history of extraction runs",
    target_fixture="db_path_with_history",
)
def db_path_with_history(db_path: Path) -> Path:
    """Create a DuckDB database with a history of extraction runs."""
    with duckdb.connect(str(db_path)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS baliza_state")
        con.execute(
            dedent(
                """
            CREATE TABLE IF NOT EXISTS baliza_state.runs (
                run_id VARCHAR PRIMARY KEY,
                resource VARCHAR,
                pipeline_name VARCHAR,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                status VARCHAR,
                windows_completed INTEGER,
                windows_failed INTEGER,
                rows_extracted INTEGER,
                error_message VARCHAR
            )
        """
            )
        )
        con.execute(
            "INSERT INTO baliza_state.runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                "run-1",
                "contratos",
                "baliza",
                "2024-01-01 10:00:00",
                "2024-01-01 10:30:00",
                "completed",
                1,
                0,
                1000,
                None
            ],
        )
        con.execute(
            "INSERT INTO baliza_state.runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                "run-2",
                "contratos",
                "baliza",
                "2024-01-02 11:00:00",
                "2024-01-02 11:15:00",
                "failed",
                0,
                1,
                50,
                "API Error"
            ],
        )
    return db_path


# --- Whens ---


@when('I run the "state show" command', target_fixture="result")
def run_state_show(db_path_with_windows: Path):
    """Run the 'state show' command."""
    result = runner.invoke(app, ["state", "show", "--duckdb", str(db_path_with_windows)])
    return result


@when('I run the "state gaps" command', target_fixture="result")
def run_state_gaps(db_path_with_windows: Path):
    """Run the 'state gaps' command."""
    result = runner.invoke(
        app,
        [
            "state",
            "gaps",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-04",
            "--duckdb",
            str(db_path_with_windows),
        ],
    )
    return result


@when('I run the "state history" command', target_fixture="result")
def run_state_history(db_path_with_history: Path):
    """Run the 'state history' command."""
    result = runner.invoke(app, ["state", "history", "--duckdb", str(db_path_with_history)])
    return result


# --- Thens ---


@then("the output should summarize the state of the data store")
def check_state_summary(result):
    """Check that the output summarizes the state of the data store."""
    assert result.exit_code == 0
    assert "Baliza PNCP Status" in result.stdout
    assert "Total contracts" in result.stdout
    # In our dummy data, we have 1 contract in baliza_raw.contratos
    assert "1" in result.stdout


@then("the output should list the missing windows")
def check_missing_windows(result):
    """Check that the output lists the missing windows."""
    assert result.exit_code == 0
    # Between 2024-01-01 and 2024-01-04, we have coverage for 01 and 03.
    # Gap is 02.
    assert "Found 1 gap(s)" in result.stdout
    assert "2024-01-02" in result.stdout


@then("the output should list the previous extraction runs")
def check_extraction_history(result):
    """Check that the output lists the previous extraction runs."""
    assert result.exit_code == 0
    assert "Extraction History" in result.stdout
    assert "run-1"[:8].lower() in result.stdout.lower()
    assert "completed" in result.stdout
    assert "run-2"[:8].lower() in result.stdout.lower()
    assert "failed" in result.stdout
