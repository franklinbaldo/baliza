from pathlib import Path
import duckdb
from pytest_bdd import scenario, given, when, then
from typer.testing import CliRunner
from baliza.cli_simple import app
import pytest

@pytest.fixture
def db_path(tmp_path):
    """Creates a temporary DuckDB database for testing."""
    return tmp_path / "test.duckdb"

@pytest.fixture
def runner():
    """Returns a Typer CliRunner."""
    return CliRunner()

@scenario('../features/state.feature', 'Show current application state')
def test_show_current_application_state():
    """Test the 'baliza state show' command."""
    pass

@given("a pre-existing database with known state", target_fixture="db_with_state")
def db_with_state(db_path: Path):
    """
    Sets up a DuckDB database with a known state.
    - Creates the necessary tables.
    - Inserts mock data to represent extraction runs and coverage.
    - Returns the path to the database file.
    """
    con = duckdb.connect(str(db_path))
    con.execute("CREATE SCHEMA IF NOT EXISTS baliza_state;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS baliza_state.extraction_runs (
            id VARCHAR,
            status VARCHAR,
            started_at TIMESTAMP,
            ended_at TIMESTAMP
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS baliza_state.cobertura (
            data_inicio DATE,
            data_fim DATE,
            total_paginas_observado INTEGER,
            status VARCHAR
        );
    """)

    con.execute("INSERT INTO baliza_state.extraction_runs VALUES ('run1', 'completed', '2024-01-01 10:00:00', '2024-01-01 11:00:00');")
    con.execute("INSERT INTO baliza_state.cobertura VALUES ('2024-01-01', '2024-01-02', 10, 'ok');")
    con.execute("INSERT INTO baliza_state.cobertura VALUES ('2024-01-02', '2024-01-03', 5, 'incompleto');")
    con.close()
    return db_path

@when('the user runs "baliza state show"', target_fixture="cli_result")
def run_state_show_command(runner: CliRunner, db_with_state: Path):
    """
    Runs the 'baliza state show' command against the test database.
    """
    result = runner.invoke(app, ["state", "show", "--duckdb", str(db_with_state)])
    return result

@then("the output should summarize the database state")
def check_output_summary(cli_result):
    """
    Asserts that the command output contains the expected state summary.
    """
    assert cli_result.exit_code == 0, f"CLI exited with an error. Stderr: {cli_result.stderr}"
    output = cli_result.stdout
    assert "Extraction History" in output
    assert "Coverage Summary" in output
    assert "completed" in output
    assert "incompleto" in output
    assert "ok" in output
