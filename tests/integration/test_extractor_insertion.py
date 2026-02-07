
import duckdb
from datetime import datetime
from unittest.mock import patch
import pytest
from baliza.extractor import PNCPExtractor

def test_extractor_insertion_logic(tmp_path):
    """
    Test that data is correctly inserted into DuckDB using the extractor.
    This verifies that the _insert_page method works correctly.
    """
    db_path = tmp_path / "test_insertion.duckdb"
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 1, 1)

    # Sample data matching the structure expected by _insert_page
    sample_data = [{
        "numeroControlePNCP": "12345678000190-1-1/2023",
        "anoCompra": 2023,
        "sequencialCompra": 1,
        "orgaoEntidade": {
            "cnpj": "12345678000190",
            "razaoSocial": "Test Org",
            "poderId": "1"
        },
        "unidadeOrgao": {
            "codigoUnidade": "123",
            "nomeUnidade": "Test Unit"
        },
        "modalidadeId": 1,
        "modalidadeNome": "Pregão",
        "valorInicial": 1000.00,
        "dataPublicacao": "2023-01-01T10:00:00",
        "dataVigenciaInicio": "2023-01-01",
        "dataVigenciaFim": "2023-12-31",
        "objetoContrato": "Test Contract",
        "informacaoComplementar": "Info",
        "numeroProcesso": "123/2023",
        "linkSistemaOrigem": "http://example.com",
        "dataInclusao": "2023-01-01T09:00:00",
        "dataAtualizacao": "2023-01-01T09:00:00",
        "usuarioNome": "User"
    }]

    with patch("baliza.extractor._fetch_page") as mock_fetch:
        # Return one page with data, then empty to stop
        mock_fetch.side_effect = [
            {
                "data": sample_data,
                "totalPaginas": 1,
                "totalRegistros": 1,
                "numeroPagina": 1
            },
            {
                "data": [],
                "totalPaginas": 1,
                "totalRegistros": 1,
                "numeroPagina": 2
            }
        ]

        with PNCPExtractor(db_path) as extractor:
            result = extractor.extract(start_date, end_date, workers=1)

        # Verify insertion
        with duckdb.connect(str(db_path)) as con:
            count = con.execute("SELECT COUNT(*) FROM baliza_raw.contratos").fetchone()[0]
            assert count == 1, "Should have inserted 1 row"

            row = con.execute("SELECT numeroControlePNCP, valorInicial FROM baliza_raw.contratos").fetchone()
            assert row[0] == "12345678000190-1-1/2023"
            assert row[1] == 1000.00
