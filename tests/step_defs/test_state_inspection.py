import pytest
import duckdb
from pytest_bdd import scenarios, given, when, then, parsers
from pathlib import Path
from typer.testing import CliRunner
from baliza.cli_simple import app

scenarios('../features/state_inspection.feature')


@pytest.fixture(scope="module")
def cli_runner():
    """Fixture for invoking command-line interfaces."""
    return CliRunner()


@pytest.fixture
def isolated_cli_runner(cli_runner, tmp_path):
    """Fixture for an isolated CLI runner with a temporary directory."""
    with cli_runner.isolated_filesystem(temp_dir=tmp_path) as td:
        yield cli_runner


@pytest.fixture
def db_path(tmp_path):
    """Path to the test database file."""
    return tmp_path / "baliza.duckdb"


@pytest.fixture
def db_connection(db_path):
    """A fixture to provide a DuckDB connection."""
    con = duckdb.connect(str(db_path))
    yield con
    con.close()


@pytest.fixture
def command_result():
    """A fixture to hold the result of the CLI command."""
    return {}


@pytest.fixture
def setup_state_tables(db_connection, db_path):
    """Creates and tears down the state tables for a test."""
    db_connection.execute("CREATE SCHEMA IF NOT EXISTS baliza_state")
    db_connection.execute("""
        CREATE TABLE IF NOT EXISTS baliza_state.windows (
            window_id VARCHAR, run_id VARCHAR, resource_name VARCHAR,
            start_date DATE, end_date DATE, status VARCHAR,
            num_pages_expected INTEGER, num_pages_completed INTEGER,
            num_records INTEGER, started_at TIMESTAMP, ended_at TIMESTAMP
        )
    """)
    db_connection.execute("""
        CREATE TABLE IF NOT EXISTS baliza_state.extraction_runs (
            run_id VARCHAR, resource_name VARCHAR, started_at TIMESTAMP, ended_at TIMESTAMP,
            status VARCHAR, lookback_days INTEGER, num_windows INTEGER,
            num_successful_windows INTEGER, num_failed_windows INTEGER
        )
    """)
    db_connection.commit()
    yield
    # Use a new connection for teardown, as the test may have closed the original one.
    with duckdb.connect(str(db_path)) as teardown_con:
        teardown_con.execute("DROP TABLE IF EXISTS baliza_state.windows")
        teardown_con.execute("DROP TABLE IF EXISTS baliza_state.extraction_runs")
        teardown_con.execute("DROP SCHEMA IF EXISTS baliza_state")
        teardown_con.commit()


@given(parsers.parse('a resource "{resource_name}" with complete, incomplete, and suspect windows'))
def given_resource_with_windows(db_connection, setup_state_tables, resource_name):
    windows_data = [
        ('w1', 'r1', resource_name, '2024-01-01', '2024-01-02', 'completed', 10, 10, 100, None, None),
        ('w2', 'r1', resource_name, '2024-01-03', '2024-01-04', 'incomplete', 10, 5, 50, None, None),
        ('w3', 'r1', resource_name, '2024-01-05', '2024-01-06', 'suspect', 10, 10, 95, None, None),
        ('w4', 'r1', resource_name, '2024-01-07', '2024-01-08', 'completed', 5, 5, 50, None, None),
    ]
    db_connection.executemany(
        "INSERT INTO baliza_state.windows VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        windows_data
    )
    db_connection.commit()


@given(parsers.parse('a resource "{resource_name}" with known coverage gaps'))
def given_resource_with_gaps(db_connection, setup_state_tables, resource_name):
    windows_data = [
        ('w1', 'r1', resource_name, '2024-01-01', '2024-01-02', 'completed', 10, 10, 100, None, None),
        # Gap from 2024-01-02 to 2024-01-05
        ('w2', 'r1', resource_name, '2024-01-05', '2024-01-06', 'completed', 10, 10, 100, None, None),
    ]
    db_connection.executemany(
        "INSERT INTO baliza_state.windows VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        windows_data
    )
    db_connection.commit()


@given(parsers.parse('a resource "{resource_name}" with a history of extraction runs'))
def given_resource_with_history(db_connection, setup_state_tables, resource_name):
    runs_data = [
        ('run1', resource_name, '2024-01-10 10:00:00', '2024-01-10 10:30:00', 'completed', 3, 5, 5, 0),
        ('run2', resource_name, '2024-01-11 10:00:00', '2024-01-11 10:15:00', 'failed', 3, 5, 2, 3),
        ('run3', resource_name, '2024-01-12 10:00:00', '2024-01-12 11:00:00', 'completed', 3, 10, 10, 0),
    ]
    db_connection.executemany(
        "INSERT INTO baliza_state.extraction_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        runs_data
    )
    db_connection.commit()


@when(parsers.parse('I run "{command}"'), target_fixture="command_result")
def when_i_run_command(command, isolated_cli_runner, db_connection, db_path):
    """Runs a CLI command and returns the result."""
    # Split the command into arguments for the runner
    args = command.split()[1:]

    # Close the connection to release the lock on the db file before the CLI runs.
    db_connection.close()

    # Explicitly pass the db path to the command.
    args.extend(["--duckdb", str(db_path)])

    result = isolated_cli_runner.invoke(app, args)
    return {
        "stdout": result.stdout,
        "stderr": result.stderr,
        "exit_code": result.exit_code,
    }


@then(parsers.parse("the output should summarize the state of the windows"))
def then_output_summarizes_state(command_result):
    assert "completed" in command_result["stdout"] and "2" in command_result["stdout"]
    assert "incomplete" in command_result["stdout"] and "1" in command_result["stdout"]
    assert "suspect" in command_result["stdout"] and "1" in command_result["stdout"]


@then(parsers.parse("the output should list the identified coverage gaps"))
def then_output_lists_gaps(command_result):
    assert "2024-01-02" in command_result["stdout"]
    assert "2024-01-05" in command_result["stdout"]
    assert "3 days" in command_result["stdout"]


@then(parsers.parse("the output should show the history of extraction runs"))
def then_output_shows_history(command_result):
    assert "run1" in command_result["stdout"]
    assert "completed" in command_result["stdout"]
    assert "run2" in command_result["stdout"]
    assert "failed" in command_result["stdout"]


@then(parsers.parse("the command should exit successfully"))
def then_command_should_exit_successfully(command_result):
    assert command_result["exit_code"] == 0, \
        f"Command failed with exit code {command_result['exit_code']}: {command_result['stderr']}"
