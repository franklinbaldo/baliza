from pathlib import Path

import duckdb
import pytest
from pytest_bdd import given, when, then, scenario
from typer.testing import CliRunner

from baliza.cli import app

runner = CliRunner()


@scenario('../features/backfill.feature', 'Backfilling data for a given month')
def test_backfilling_data_for_a_given_month():
    pass


@given("a clean DuckDB database", target_fixture="db_path")
def db_path(tmp_path: Path) -> Path:
    db = tmp_path / "test.duckdb"
    if db.exists():
        db.unlink()
    return db


@when("I run the baliza backfill command for a specific month", target_fixture="run_result")
def run_backfill_command(db_path, patched_pncp_source):
    result = runner.invoke(
        app,
        [
            "backfill",
            "2024-01",
            "2024-01",
            "--duckdb",
            str(db_path),
            "--dataset",
            "test_dataset",
        ],
    )
    assert result.exit_code == 0
    return result


@then("the data for that month should be extracted and saved to the DuckDB database")
def check_data_in_db(db_path):
    with duckdb.connect(str(db_path), read_only=True) as con:
        count = con.execute("SELECT COUNT(*) FROM test_dataset.contratos").fetchone()[0]
        assert count > 0
