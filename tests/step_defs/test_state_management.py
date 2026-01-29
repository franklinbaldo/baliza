"""Step definitions for the state management feature."""

import re
from pathlib import Path
from textwrap import dedent

import duckdb
import pytest
from pytest_bdd import given, scenario, then, when
from typer.testing import CliRunner
from pytest_bdd import parsers

from baliza.cli_simple import app

runner = CliRunner()

# --- Scenarios ---

@pytest.mark.skip(reason="Not implemented yet")
@scenario(
    "../features/state_management.feature",
    "Show the state of the data extraction process",
)
def test_show_state():
    """Show the state of the data extraction process."""

@pytest.mark.skip(reason="Not implemented yet")
@scenario(
    "../features/state_management.feature",
    "List the gaps in the data extraction process",
)
def test_list_gaps():
    """List the gaps in the data extraction process."""

@pytest.mark.skip(reason="Not implemented yet")
@scenario(
    "../features/state_management.feature",
    "Show the history of the data extraction process",
)
def test_show_history():
    """Show the history of the data extraction process."""

@pytest.mark.skip(reason="Not implemented yet")
@scenario(
    "../features/state_management.feature",
    "Resumable extraction process",
)
def test_resumable_extraction():
    """Resumable extraction process."""


# --- Givens ---

@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Create a clean DuckDB database for state management tests."""
    db = tmp_path / "test_state.duckdb"
    if db.exists():
        db.unlink()
    return db

@given(
    parsers.parse("the database contains the following coverage windows:\n{datatable}"),
    target_fixture="db_with_coverage",
)
def db_with_coverage(db_path: Path, datatable) -> Path:
    """Create a DuckDB database with a predefined coverage state."""
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
        headers = [h.strip() for h in datatable.splitlines()[0].strip().split('|')[1:-1]]
        for row_str in datatable.splitlines()[1:]:
            row = [v.strip() for v in row_str.strip().split('|')[1:-1]]
            con.execute(
                f"INSERT INTO baliza_state.coverage ({', '.join(headers)}) VALUES ({', '.join(['?'] * len(headers))})",
                row,
            )
    return db_path

@given(
    parsers.parse("the database contains the following extraction runs:\n{datatable}"),
    target_fixture="db_with_history",
)
def db_with_history(db_path: Path, datatable) -> Path:
    """Create a DuckDB database with a predefined run history."""
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
        headers = [h.strip() for h in datatable.splitlines()[0].strip().split('|')[1:-1]]
        for row_str in datatable.splitlines()[1:]:
            row = [v.strip() for v in row_str.strip().split('|')[1:-1]]
            con.execute(
                f"INSERT INTO baliza_state.extraction_runs ({', '.join(headers)}) VALUES ({', '.join(['?'] * len(headers))})",
                row,
            )
    return db_path

@given(parsers.parse('today\'s date is "{date}"'))
def mock_today_date(monkeypatch, date):
    """Mock today's date."""
    # This is a placeholder for mocking the date in the application logic
    pass

# --- Whens ---

@when(parsers.parse('I run the command "{command}"'), target_fixture="result")
def run_command(db_with_coverage: Path, command: str):
    """Run a CLI command."""
    # The db_with_coverage fixture is used to ensure the db is created
    # even if the command does not use it.
    args = command.split() + ["--duckdb", str(db_with_coverage)]
    result = runner.invoke(app, args)
    return result

# --- Thens ---

@then(parsers.parse("I should see a table summarizing the state for each resource:\n{datatable}"))
def check_state_summary_table(result, datatable):
    """Check the state summary table output."""
    assert result.exit_code == 0
    # Placeholder for table parsing and comparison
    assert "Not implemented yet" in result.stdout

@then(parsers.parse("I should see a list of the missing daily windows:\n{datatable}"))
def check_missing_windows_list(result, datatable):
    """Check the list of missing windows."""
    assert result.exit_code == 0
    # Placeholder for list parsing and comparison
    assert "Not implemented yet" in result.stdout

@then(parsers.parse("I should see a table of the previous extraction runs:\n{datatable}"))
def check_extraction_history_table(result, datatable):
    """Check the extraction history table output."""
    assert result.exit_code == 0
    # Placeholder for table parsing and comparison
    assert "Not implemented yet" in result.stdout

@then(parsers.parse("the extractor should prioritize the following windows:\n{datatable}"))
def check_extraction_priority(result, datatable):
    """Check that the extractor prioritizes the correct windows."""
    assert result.exit_code == 0
    # Placeholder for checking logs or other output
    assert "Not implemented yet" in result.stdout
