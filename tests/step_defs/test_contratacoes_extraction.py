"""Step definitions for the contratacoes extraction feature."""

from pathlib import Path
from urllib.parse import parse_qs, urlparse

import duckdb
import httpx
import pytest
from pytest_bdd import given, parsers, scenario, then, when
from typer.testing import CliRunner

from baliza.cli_simple import app

runner = CliRunner()


@scenario(
    "../features/contratacoes_extraction.feature",
    "The data pipeline can extract contratacoes",
)
def test_pipeline_can_extract_contratacoes():
    """The data pipeline can extract contratacoes."""


@given("a clean local data store", target_fixture="db_path")
def db_path(tmp_path: Path) -> Path:
    """Create a clean DuckDB database."""
    db = tmp_path / "test.duckdb"
    if db.exists():
        db.unlink()
    return db


@given(
    "the PNCP API is mocked for 'contratacoes' for a specific date range",
    target_fixture="mocked_contratacoes_api",
)
def mocked_contratacoes_api(httpx_mock):
    """Mock the external data source for the PNCP API for 'contratacoes'."""

    def mock_pncp_api(request: httpx.Request):
        """Mock the PNCP API, responding based on the resource and date range."""
        parsed_url = urlparse(str(request.url))

        if "/contratacoes" not in parsed_url.path:
            return httpx.Response(404, json={"error": "Not Found"})

        # Data for contratacoes
        record_1 = {
            "idContratacao": "CONTR-1",
            "numeroControlePNCP": "PNCP-CONTR-1",
            "objeto": "Objeto da Contratação 1",
            "dataPublicacao": "2024-02-01",
        }
        record_2 = {
            "idContratacao": "CONTR-2",
            "numeroControlePNCP": "PNCP-CONTR-2",
            "objeto": "Objeto da Contratação 2",
            "dataPublicacao": "2024-02-02",
        }

        return httpx.Response(
            200, json={"data": [record_1, record_2], "totalPaginas": 1}
        )

    httpx_mock.add_callback(mock_pncp_api, is_reusable=True)


@when(parsers.parse("I extract '{resource}' data for the date range"))
def extract_data(db_path, mocked_contratacoes_api, resource: str):
    """Extract data for the given resource and date range."""
    result = runner.invoke(
        app,
        [
            "extract",
            "--start",
            "2024-02-01",
            "--end",
            "2024-02-02",
            "--duckdb",
            str(db_path),
            "--dataset",
            "test_dataset",
            "--resource",
            resource,
        ],
    )
    assert result.exit_code == 0, f"Command failed: {result.stdout}"


@then(
    parsers.parse(
        "the final dataset should contain all '{resource}' records for the date range"
    )
)
def check_all_records(db_path, resource: str):
    """Verify that the final dataset contains all records for the resource."""
    with duckdb.connect(str(db_path), read_only=True) as con:
        try:
            table_name = f"test_dataset.{resource}"
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            assert count == 2, f"Expected 2 records in {table_name}, but found {count}"
        except duckdb.CatalogException:
            pytest.fail(f"Table {table_name} does not exist in the database.")
