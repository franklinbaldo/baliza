import os
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest
from pytest_bdd import given, when, then, scenario, parsers
from typer.testing import CliRunner

from baliza.cli import app


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    db = tmp_path / "test.duckdb"
    if db.exists():
        db.unlink()
    return db




def setup_test_db(db_path: Path):
    with duckdb.connect(str(db_path)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS test_dataset;")
        con.execute("CREATE TABLE IF NOT EXISTS test_dataset.contratos (id INT);")
        con.execute("DELETE FROM test_dataset.contratos;")


def insert_records(db_path: Path, count: int, start: int = 0):
    with duckdb.connect(str(db_path)) as con:
        con.execute(f"INSERT INTO test_dataset.contratos SELECT * FROM range({start}, {start + count});")


@scenario('../features/extraction.feature', 'Successful extraction for a date range')
def test_successful_extraction():
    pass


@scenario('../features/extraction.feature', 'Failed extraction due to API error')
def test_failed_extraction():
    pass


@scenario('../features/extraction.feature', 'Resumed extraction after a failure')
def test_resumed_extraction():
    pass


@given("a clean DuckDB database")
def clean_db(db_path):
    setup_test_db(db_path)


@given("a mock PNCP API that will succeed")
def mock_api_succeed(mocker, db_path):
    def mock_run_pncp(*args, **kwargs):
        # This mock simulates the final outcome of a successful run over the given date range.
        # It clears the table and inserts the total expected records.
        setup_test_db(db_path)
        insert_records(db_path, 1000)
        return MagicMock(), MagicMock()

    mocker.patch('baliza.cli.run_pncp', side_effect=mock_run_pncp)


@given("a mock PNCP API that will fail on the second call")
def mock_api_fail(mocker, db_path):
    def mock_run_pncp_with_failure(*args, **kwargs):
        # This mock simulates a partial run before failing.
        setup_test_db(db_path)
        insert_records(db_path, 500)
        raise Exception("Simulated API Error")

    mocker.patch('baliza.cli.run_pncp', side_effect=mock_run_pncp_with_failure)


@given("a database with a partially completed extraction")
def partial_db(db_path):
    setup_test_db(db_path)
    insert_records(db_path, 500)


@when(parsers.parse('I run the baliza extract command for the dates "{start_date}" to "{end_date}"'))
def run_command(runner, db_path, start_date, end_date, run_result):
    # We use a test mode env var to make the CLI re-raise exceptions for testing.
    os.environ['BALIZA_TEST_MODE'] = '1'
    try:
        result = runner.invoke(
            app,
            [
                "extract",
                "--duckdb", str(db_path),
                "--start-date", start_date,
                "--end-date", end_date,
                "--dataset", "test_dataset",
            ],
            catch_exceptions=False  # This helps catch programmer errors in the CLI code itself
        )
        run_result['result'] = result
    except Exception as e:
        # If an exception is raised by the app (due to test mode), we catch it here.
        from typer.testing import CliRunner
        result = CliRunner().invoke(app, ["--help"]) # create a dummy result
        result.exit_code = 1 # make it non-zero
        result.exception = e
        run_result['result'] = result
    finally:
        del os.environ['BALIZA_TEST_MODE']


@then('the command should succeed')
def check_command_succeeded(run_result):
    assert run_result['result'].exit_code == 0


@then('the command should fail')
def check_command_failed(run_result):
    assert run_result['result'].exit_code != 0
    assert isinstance(run_result['result'].exception, Exception)


@then(parsers.parse('the database should contain "{count}" contracts for the resource "{resource}"'))
def check_data_in_db(db_path, count, resource):
    with duckdb.connect(str(db_path), read_only=True) as con:
        table_name = f"test_dataset.{resource}"
        actual_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        assert actual_count == int(count)
