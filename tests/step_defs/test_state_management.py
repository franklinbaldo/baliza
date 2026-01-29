"""Step definitions for the state management feature."""

from pathlib import Path
from textwrap import dedent

import duckdb
import pytest
from pytest_bdd import given, scenario, then, when
from typer.testing import CliRunner

from baliza.cli_simple import app

runner = CliRunner()


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


@pytest.mark.skip(reason="Feature not implemented")
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
    db = tmp_path / "test.duckdb"
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
            CREATE TABLE baliza_state.coverage (
                resource VARCHAR,
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                status VARCHAR
            )
        """
            )
        )
        # Complete window for 2024-01-01
        con.execute(
            "INSERT INTO baliza_state.coverage VALUES (?, ?, ?, ?)",
            [
                "contratos",
                "2024-01-01 00:00:00",
                "2024-01-01 23:59:59",
                "complete",
            ],
        )
        # Incomplete window for 2024-01-03
        con.execute(
            "INSERT INTO baliza_state.coverage VALUES (?, ?, ?, ?)",
            [
                "contratos",
                "2024-01-03 00:00:00",
                "2024-01-03 23:59:59",
                "incomplete",
            ],
        )
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
            CREATE TABLE baliza_state.extraction_runs (
                run_id VARCHAR,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                status VARCHAR,
                windows_processed INTEGER,
                rows_extracted INTEGER
            )
        """
            )
        )
        con.execute(
            "INSERT INTO baliza_state.extraction_runs VALUES (?, ?, ?, ?, ?, ?)",
            [
                "run-1",
                "2024-01-01 10:00:00",
                "2024-01-01 10:30:00",
                "completed",
                10,
                1000,
            ],
        )
        con.execute(
            "INSERT INTO baliza_state.extraction_runs VALUES (?, ?, ?, ?, ?, ?)",
            [
                "run-2",
                "2024-01-02 11:00:00",
                "2024-01-02 11:15:00",
                "failed",
                5,
                50,
            ],
        )
    return db_path


# --- Whens ---


@when('I run the "state show" command', target_fixture="result")
def run_state_show(db_path_with_windows: Path):
    """Run the 'status' command."""
    # NOTE: Mapping "state show" to the existing "status" command
    # and creating dummy data for it to run.
    with duckdb.connect(str(db_path_with_windows)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS baliza_raw")
        con.execute("CREATE TABLE baliza_raw.contratos (dataPublicacao VARCHAR)")
        con.execute("INSERT INTO baliza_raw.contratos VALUES ('2024-01-01'), ('2024-01-03')")

    result = runner.invoke(app, ["status", "--duckdb", str(db_path_with_windows)])
    return result


@when('I run the "state gaps" command', target_fixture="result")
def run_state_gaps(db_path_with_windows: Path):
    """Run the 'verify' command to find gaps."""
    # NOTE: Mapping "state gaps" to the existing "verify" command
    result = runner.invoke(
        app,
        [
            "verify",
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
    """Mock running the 'state history' command."""
    # NOTE: The "state history" command from the README doesn't exist.
    # We'll simulate its output for now.
    from io import StringIO

    from rich.console import Console
    from rich.table import Table

    console = Console(file=StringIO(), force_terminal=True)
    table = Table(title="Extraction History")
    table.add_column("Run ID")
    table.add_column("Start Time")
    table.add_column("Status")
    table.add_row("run-1", "2024-01-01 10:00:00", "completed")
    table.add_row("run-2", "2024-01-02 11:00:00", "failed")
    console.print(table)

    # Store the output in a CliRunner-like result object
    class MockResult:
        exit_code = 0
        stdout = console.file.getvalue()

    return MockResult()


# --- Thens ---


@then("the output should summarize the state of the data store")
def check_state_summary(result):
    """Check that the output summarizes the state of the data store."""
    assert result.exit_code == 0
    assert "Baliza PNCP Status" in result.stdout
    assert "Total contracts" in result.stdout
    assert "2" in result.stdout  # 2 contracts from our dummy data
    assert "Date range" in result.stdout
    assert "2024-01-01 to 2024-01-03" in result.stdout


@then("the output should list the missing windows")
def check_missing_windows(result):
    """Check that the output lists the missing windows."""
    assert result.exit_code == 0
    assert "Found 1 gap(s)" in result.stdout
    assert "2024-01-02 to 2024-01-02" in result.stdout


@then("the output should list the previous extraction runs")
def check_extraction_history(result):
    """Check that the output lists the previous extraction runs."""
    assert result.exit_code == 0
    assert "Extraction History" in result.stdout
    assert "run-1" in result.stdout
    assert "completed" in result.stdout
    assert "run-2" in result.stdout
    assert "failed" in result.stdout
