
from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

import duckdb
from pytest_bdd import given, parsers, scenarios, then, when
from typer.testing import CliRunner

from baliza.cli_simple import app
from baliza.extractor import PNCPExtractor

runner = CliRunner()

scenarios("../features/contratacoes_extraction.feature")


@given("the PNCP API will return a list of contratacoes_publicacao")
def mock_api_contratacoes_publicacao_happy_path(httpx_mock):
    """Mock the PNCP API to return a sample contratacoes_publicacao response."""
    sample_response = {
        "data": [
            {
                "numeroControlePNCP": "01613093000192-1-000008/2024",
                "anoCompra": 2024,
                "sequencialCompra": 8,
                "dataAtualizacao": "2024-12-01T00:20:32",
                "orgaoEntidade": {"cnpj": "01613093000192", "razaoSocial": "MUNICIPIO DE MONTE SANTO DO TOCANTINS", "poderId": "N", "esferaId": "M"},
                "unidadeOrgao": {"ufNome": "Tocantins", "codigoUnidade": "1720", "ufSigla": "TO", "municipioNome": "Monte Santo do Tocantins", "nomeUnidade": "MUNICIPIO DE MONTE SANTO DO TOCANTINS/TO", "codigoIbge": "1713700"},
                "numeroCompra": "12",
                "processo": "1805/2024",
                "objetoCompra": "[LICITANET] - AQUISIÇÃO DE PÁ CARREGADEIRA...",
                "valorTotalHomologado": 370000.0,
                "srp": False,
                "dataInclusao": "2024-12-01T00:20:32",
                "amparoLegal": {"codigo": 1, "nome": "Lei 14.133/2021, Art. 28, I", "descricao": "pregão..."},
                "dataAberturaProposta": "2024-12-01T00:20:28",
                "dataEncerramentoProposta": "2024-12-12T08:00:00",
                "informacaoComplementar": "",
                "linkSistemaOrigem": "https://portal.licitanet.com.br/acesso-visitante/WkpTbGxwU2o=",
                "justificativaPresencial": "",
                "dataPublicacaoPncp": "2024-12-01T00:20:32",
                "modalidadeId": 6,
                "dataAtualizacaoGlobal": "2024-12-26T09:27:41",
                "linkProcessoEletronico": None,
                "modoDisputaId": 1,
                "tipoInstrumentoConvocatorioNome": "Edital",
                "tipoInstrumentoConvocatorioCodigo": 1,
                "modalidadeNome": "Pregão - Eletrônico",
                "valorTotalEstimado": 602333.0,
                "modoDisputaNome": "Aberto",
                "situacaoCompraId": 1,
                "situacaoCompraNome": "Divulgada no PNCP",
                "usuarioNome": "Licitanet Licitações Eletrônicas LTDA"
            }
        ],
        "totalPaginas": 1,
    }
    httpx_mock.add_response(
        method="GET",
        url=re.compile(r".*/contratacoes_publicacao.*"),
        json=sample_response,
    )


@given("the PNCP API will return an empty list for contratacoes_publicacao")
def mock_api_contratacoes_publicacao_empty(httpx_mock):
    """Mock the PNCP API to return an empty response."""
    httpx_mock.add_response(
        method="GET",
        url=re.compile(r".*/contratacoes_publicacao.*"),
        json={"data": [], "totalPaginas": 0},
    )


@when(parsers.parse('I run the extract command for "{resource}"'))
def run_extract_command(tmp_path: Path, resource: str):
    """Run the extract command for a given resource."""
    db_path = tmp_path / "test.db"
    result = runner.invoke(
        app,
        [
            "extract",
            "--duckdb",
            str(db_path),
            "--resource",
            resource,
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-01",
        ],
    )
    assert result.exit_code == 0


@then(parsers.parse('the "{table_name}" table should contain the extracted data'))
def check_table_has_data(tmp_path: Path, table_name: str):
    """Check if the specified table contains data."""
    db_path = tmp_path / "test.db"
    with duckdb.connect(str(db_path)) as con:
        count_result = con.execute(f"SELECT COUNT(*) FROM baliza_raw.{table_name}").fetchone()
        assert count_result is not None
        assert count_result[0] > 0


@then(parsers.parse('the "{table_name}" table should be empty'))
def check_table_is_empty(tmp_path: Path, table_name: str):
    """Check if the specified table is empty."""
    db_path = tmp_path / "test.db"
    with duckdb.connect(str(db_path)) as con:
        # First ensure the table exists
        extractor = PNCPExtractor(db_path)
        extractor._ensure_schema(con, table_name)

        count_result = con.execute(f"SELECT COUNT(*) FROM baliza_raw.{table_name}").fetchone()
        assert count_result is not None
        assert count_result[0] == 0
