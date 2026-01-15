from pathlib import Path

import duckdb
import pytest
from pytest_bdd import given, when, then, scenario
from typer.testing import CliRunner

from baliza.cli import app

runner = CliRunner()


@scenario('../features/verification.feature', 'Verifying coverage for a period with complete data')
def test_verifying_coverage_for_a_period_with_complete_data():
    pass


@scenario('../features/verification.feature', 'Verifying coverage for a period with gaps')
def test_verifying_coverage_for_a_period_with_gaps():
    pass


import json
from unittest.mock import MagicMock
from contextlib import contextmanager

from baliza import cli as baliza_cli


@pytest.fixture
def mock_http_client_factory(monkeypatch):
    @contextmanager
    def factory(*args, **kwargs):
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": [], "totalPaginas": 1}
        mock_client.get.return_value = mock_response
        yield mock_client

    monkeypatch.setattr(baliza_cli, "_HTTP_CLIENT_FACTORY", factory)
    return factory


@given("a DuckDB database with complete data for a specific period", target_fixture="db_path")
def db_path_complete(tmp_path: Path) -> Path:
    db_path = tmp_path / "test_complete.duckdb"
    if db_path.exists():
        db_path.unlink()

    with duckdb.connect(str(db_path)) as con:
        # State schema
        con.execute("CREATE SCHEMA IF NOT EXISTS baliza_state")
        con.execute("""
            CREATE TABLE baliza_state.cobertura (
                recurso TEXT, janela_inicio TIMESTAMP, janela_fim TIMESTAMP, pagina INTEGER,
                total_paginas_observado INTEGER, n_registros_pagina INTEGER,
                hash_ids TEXT, fetched_at TIMESTAMP
            )
        """)
        con.execute("""
            INSERT INTO baliza_state.cobertura VALUES
            ('contratos', '2024-01-01', '2024-01-02', 1, 1, 1, 'hash', '2024-01-01'),
            ('contratos', '2024-01-02', '2024-01-03', 1, 1, 1, 'hash', '2024-01-02'),
            ('contratos', '2024-01-03', '2024-01-04', 1, 1, 1, 'hash', '2024-01-03'),
            ('contratos', '2024-01-04', '2024-01-05', 1, 1, 1, 'hash', '2024-01-04');
        """)
        # Raw data schema
        con.execute("CREATE SCHEMA IF NOT EXISTS baliza_raw")
        con.execute("CREATE TABLE baliza_raw.contratos (dataPublicacaoPncp TIMESTAMP)")
        con.execute("""
            INSERT INTO baliza_raw.contratos VALUES ('2024-01-01'), ('2024-01-02'), ('2024-01-03'), ('2024-01-04');
        """)
    return db_path


@given("a DuckDB database with data gaps for a specific period", target_fixture="db_path")
def db_path_gaps(tmp_path: Path) -> Path:
    db_path = tmp_path / "test_gaps.duckdb"
    if db_path.exists():
        db_path.unlink()

    with duckdb.connect(str(db_path)) as con:
        # State schema
        con.execute("CREATE SCHEMA IF NOT EXISTS baliza_state")
        con.execute("""
            CREATE TABLE baliza_state.cobertura (
                recurso TEXT, janela_inicio TIMESTAMP, janela_fim TIMESTAMP, pagina INTEGER,
                total_paginas_observado INTEGER, n_registros_pagina INTEGER,
                hash_ids TEXT, fetched_at TIMESTAMP
            )
        """)
        con.execute("""
            INSERT INTO baliza_state.cobertura VALUES
            ('contratos', '2024-01-01', '2024-01-02', 1, 1, 1, 'hash', '2024-01-01'),
            ('contratos', '2024-01-04', '2024-01-05', 1, 1, 1, 'hash', '2024-01-04');
        """)
        # Raw data schema
        con.execute("CREATE SCHEMA IF NOT EXISTS baliza_raw")
        con.execute("CREATE TABLE baliza_raw.contratos (dataPublicacaoPncp TIMESTAMP)")
        con.execute("""
            INSERT INTO baliza_raw.contratos VALUES ('2024-01-01'), ('2024-01-02'), ('2024-01-03'), ('2024-01-04');
        """)
    return db_path


@when("I run the baliza verify command for that period", target_fixture="run_result")
def run_verify_command(db_path: Path, mock_http_client_factory):
    result = runner.invoke(
        app,
        [
            "verify",
            "--duckdb",
            str(db_path),
            "--resource",
            "contratos",
            "--desde",
            "2024-01-01",
            "--ate",
            "2024-01-05",
        ],
    )
    return result


@then("the verification should pass successfully")
def check_verification_success(run_result):
    assert run_result.exit_code == 0
    data = json.loads(run_result.stdout)
    assert data["lacunas"] == []
    assert data["suspeitas"] == []


@then("the verification should identify the gaps")
def check_verification_gaps(run_result):
    assert run_result.exit_code == 0
    data = json.loads(run_result.stdout)
    assert len(data["lacunas"]) > 0
