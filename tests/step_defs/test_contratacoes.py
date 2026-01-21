
from pytest_bdd import scenarios, given, when, then, parsers
from typer.testing import CliRunner
from baliza.cli_simple import app
import duckdb
import pytest
from pathlib import Path

scenarios('../features/contratacoes.feature')

@pytest.fixture
def runner():
    return CliRunner()

@pytest.fixture
def db_path(tmp_path):
    db = tmp_path / "test.db"
    if db.exists():
        db.unlink()
    return db

@given("a clean database")
def a_clean_database(db_path):
    # The db_path fixture already handles this
    pass

@given('a mock PNCP API for "contratacoes"')
def mock_pncp_api_contratacoes(httpx_mock):
    httpx_mock.add_response(
        method="GET",
        url="https://pncp.gov.br/api/consulta/v1/contratacoes?dataInicial=20241201&dataFinal=20241201&pagina=1&tamanhoPagina=500",
        json={
            "data": [
                {
                    "numeroControlePNCP": "08084014000142-1-000057/2024",
                    "anoCompra": 2024,
                    "sequencialCompra": 57,
                    "objetoCompra": "AQUISIÇÃO DE MATERIAIS PERMANENTE",
                    "valorTotalEstimado": 151192.18,
                    "dataPublicacaoPncp": "2024-12-01T04:46:07",
                }
            ],
            "totalPaginas": 1,
        },
        status_code=200,
    )

@when(parsers.parse('I run the extract command for "{resource}" from "{start_date}" to "{end_date}"'))
def run_extract_command(runner, db_path, resource, start_date, end_date):
    result = runner.invoke(
        app,
        ["extract", "--start", start_date, "--end", end_date, "--resource", resource, "--duckdb", str(db_path)],
    )
    assert result.exit_code == 0


@then(parsers.parse('the "{table_name}" table should contain {num_records:d} record'))
def check_table_records(db_path, table_name, num_records):
    with duckdb.connect(str(db_path)) as con:
        try:
            count_result = con.execute(f"SELECT COUNT(*) FROM baliza_raw.{table_name}").fetchone()
            assert count_result[0] == num_records
        except duckdb.CatalogException:
            # Table doesn't exist, which is a failure
            assert 0 == num_records
