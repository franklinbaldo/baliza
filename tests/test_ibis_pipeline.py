import ibis
import pytest
from pipelines.ibis_pipeline import run_ibis_pipeline

@pytest.fixture(scope="module")
def con():
    """In-memory DuckDB connection for testing."""
    return ibis.connect("duckdb://")

@pytest.fixture(scope="module")
def setup_data(con):
    """Create dummy bronze data for testing."""
    con.create_table(
        "bronze_pncp_raw",
        ibis.memtable(
            [
                {
                    "numeroControlePNCP": "123",
                    "anoContratacao": 2024,
                    "dataPublicacaoPNCP": "2024-01-01",
                    "dataAtualizacaoPNCP": "2024-01-01",
                    "horaAtualizacaoPNCP": "12:00:00",
                    "sequencialOrgao": 1,
                    "cnpjOrgao": "12345678901234",
                    "siglaOrgao": "TEST",
                    "nomeOrgao": "Test Organization",
                    "sequencialUnidade": 1,
                    "codigoUnidade": "001",
                    "siglaUnidade": "TU",
                    "nomeUnidade": "Test Unit",
                    "codigoEsfera": "1",
                    "nomeEsfera": "Federal",
                    "codigoPoder": "1",
                    "nomePoder": "Executivo",
                    "codigoMunicipio": "5300108",
                    "nomeMunicipio": "Brasilia",
                    "uf": "DF",
                    "amparoLegalId": "1",
                    "amparoLegalNome": "Test Legal",
                    "modalidadeId": "1",
                    "modalidadeNome": "Test Mode",
                    "numeroContratacao": "001/2024",
                    "processo": "123/2024",
                    "objetoContratacao": "Test Object",
                    "codigoSituacaoContratacao": "1",
                    "nomeSituacaoContratacao": "Active",
                    "valorTotalEstimado": 1000.00,
                    "informacaoComplementar": "None",
                    "dataAssinatura": "2024-01-01",
                    "dataVigenciaInicio": "2024-01-01",
                    "dataVigenciaFim": "2024-12-31",
                    "numeroControlePNCPContrato": "456",
                    "justificativa": "Test Justification",
                    "fundamentacaoLegal": "Test Legal",
                }
            ]
        ),
    )


def test_run_ibis_pipeline(con, setup_data):
    """Test the full Ibis pipeline execution."""
    run_ibis_pipeline(con)

    # Verify that the tables were created
    assert "silver_contratacoes" in con.list_tables()
    assert "silver_dim_unidades_orgao" in con.list_tables()
    assert "gold_contratacoes_analytics" in con.list_tables()

    # Verify the content of the silver table
    silver_contratacoes = con.table("silver_contratacoes")
    assert silver_contratacoes.count().execute() == 1

    # Verify the content of the gold table
    gold_analytics = con.table("gold_contratacoes_analytics")
    assert gold_analytics.count().execute() == 1
    result = gold_analytics.execute()
    assert result["quantidade_contratacoes"][0] == 1
    assert result["valor_total_estimado"][0] == 1000.00
