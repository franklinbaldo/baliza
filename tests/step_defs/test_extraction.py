from pathlib import Path

import duckdb
import pytest
from dlt.sources import DltResource
from pytest_bdd import given, when, then, scenario
from typer.testing import CliRunner
from dlt.common import pendulum

from baliza.cli import app
from baliza.pipelines import pncp

runner = CliRunner()


def parse_dates(item):
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


@pytest.mark.vcr
@scenario('../features/extraction.feature', 'Extracting data for a given period')
def test_extracting_data_for_a_given_period():
    pass


@given("a clean DuckDB database", target_fixture="db_path")
def db_path(tmp_path: Path) -> Path:
    db = tmp_path / "test.duckdb"
    if db.exists():
        db.unlink()
    return db


@when("I run the baliza extract command for a specific date range", target_fixture="run_result")
def run_extract_command(db_path, patched_pncp_source):
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
    return result


@then("the data should be extracted and saved to the DuckDB database")
def check_data_in_db(db_path):
    with duckdb.connect(str(db_path), read_only=True) as con:
        count = con.execute("SELECT COUNT(*) FROM test_dataset.contratos").fetchone()[0]
        assert count > 0
