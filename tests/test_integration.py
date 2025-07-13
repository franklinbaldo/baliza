"""
Integration tests for Baliza end-to-end data pipeline.
"""
import pytest
import tempfile
import os
import json
import duckdb
from datetime import date, timedelta
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import sys

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from baliza.main import (
    run_baliza, harvest_endpoint_data, process_and_upload_data, 
    _init_baliza_db, _load_to_psa, ENDPOINTS_CONFIG
)
from baliza.ia_federation import InternetArchiveFederation


@pytest.fixture
def temp_workspace():
    """Create a temporary workspace for integration tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Change to temp directory
        original_cwd = os.getcwd()
        os.chdir(tmpdir)
        
        # Create necessary directories
        os.makedirs("state", exist_ok=True)
        os.makedirs("baliza_data", exist_ok=True)
        
        yield tmpdir
        
        # Restore original directory
        os.chdir(original_cwd)


@pytest.fixture
def mock_pncp_response():
    """Mock PNCP API response with realistic data."""
    return {
        "data": [
            {
                "numeroControlePncpCompra": "12345-2024-001",
                "anoContrato": 2024,
                "dataAssinatura": "20240115",
                "niFornecedor": "12345678000195",
                "nomeRazaoSocialFornecedor": "Empresa Teste LTDA",
                "objetoContrato": "Prestação de serviços de TI",
                "valorInicial": 50000.00,
                "valorGlobal": 50000.00,
                "orgaoEntidade": {
                    "razaoSocial": "Prefeitura Municipal",
                    "cnpj": "11111111000111",
                    "uf": "RO"
                },
                "tipoContrato": {
                    "codigo": "1",
                    "descricao": "Serviços"
                }
            },
            {
                "numeroControlePncpCompra": "12345-2024-002", 
                "anoContrato": 2024,
                "dataAssinatura": "20240116",
                "niFornecedor": "98765432000123",
                "nomeRazaoSocialFornecedor": "Outra Empresa SA",
                "objetoContrato": "Fornecimento de materiais",
                "valorInicial": 75000.00,
                "valorGlobal": 75000.00,
                "orgaoEntidade": {
                    "razaoSocial": "Governo do Estado",
                    "cnpj": "22222222000222",
                    "uf": "RO"
                },
                "tipoContrato": {
                    "codigo": "2",
                    "descricao": "Materiais"
                }
            }
        ],
        "totalRegistros": 2,
        "totalPaginas": 1,
        "paginaAtual": 1
    }


def test_database_initialization(temp_workspace):
    """Test database initialization creates all required schemas and tables."""
    _init_baliza_db()
    
    # Verify database file was created
    assert os.path.exists("state/baliza.duckdb")
    
    # Verify schemas and tables
    conn = duckdb.connect("state/baliza.duckdb")
    
    # Check schemas
    schemas = conn.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
    schema_names = [s[0] for s in schemas]
    assert 'psa' in schema_names
    assert 'control' in schema_names
    assert 'federated' in schema_names
    
    # Check PSA tables
    psa_tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'psa'").fetchall()
    psa_table_names = [t[0] for t in psa_tables]
    assert 'contratos_raw' in psa_table_names
    
    # Check control tables
    control_tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'control'").fetchall()
    control_table_names = [t[0] for t in control_tables]
    assert 'runs' in control_table_names
    assert 'data_quality' in control_table_names
    
    conn.close()


@patch('baliza.main.fetch_data_from_pncp')
def test_harvest_endpoint_data_success(mock_fetch, temp_workspace, mock_pncp_response):
    """Test successful data harvesting from PNCP endpoint."""
    mock_fetch.return_value = mock_pncp_response
    
    endpoint_key = "contratos_publicacao"
    endpoint_cfg = ENDPOINTS_CONFIG[endpoint_key]
    test_date = "2024-01-15"
    
    records = harvest_endpoint_data(test_date, endpoint_key, endpoint_cfg)
    
    assert records is not None
    assert len(records) == 2
    assert records[0]["numeroControlePncpCompra"] == "12345-2024-001"
    assert records[1]["numeroControlePncpCompra"] == "12345-2024-002"


@patch('baliza.main.fetch_data_from_pncp')
def test_harvest_endpoint_data_no_data(mock_fetch, temp_workspace):
    """Test harvesting when no data is available."""
    mock_fetch.return_value = {
        "data": [],
        "totalRegistros": 0,
        "totalPaginas": 0,
        "paginaAtual": 1
    }
    
    endpoint_key = "contratos_publicacao"
    endpoint_cfg = ENDPOINTS_CONFIG[endpoint_key]
    test_date = "2024-01-15"
    
    records = harvest_endpoint_data(test_date, endpoint_key, endpoint_cfg)
    
    assert records == []


def test_psa_loading(temp_workspace, mock_pncp_response):
    """Test loading data to Persistent Staging Area."""
    _init_baliza_db()
    
    test_records = mock_pncp_response["data"]
    run_id = "test_run_001"
    data_date = "2024-01-15"
    endpoint_key = "contratos_publicacao"
    
    records_inserted = _load_to_psa(run_id, data_date, endpoint_key, test_records)
    
    assert records_inserted == 2
    
    # Verify data was inserted
    conn = duckdb.connect("state/baliza.duckdb")
    result = conn.execute("""
        SELECT COUNT(*) FROM psa.contratos_raw 
        WHERE baliza_run_id = ?
    """, [run_id]).fetchone()
    
    assert result[0] == 2
    
    # Verify specific data fields
    record = conn.execute("""
        SELECT numeroControlePncpCompra, valorInicial, orgaoEntidade_json
        FROM psa.contratos_raw 
        WHERE baliza_run_id = ? 
        LIMIT 1
    """, [run_id]).fetchone()
    
    assert record[0] == "12345-2024-001"
    assert record[1] == 50000.00
    assert json.loads(record[2])["razaoSocial"] == "Prefeitura Municipal"
    
    conn.close()


@patch('baliza.main.upload')
@patch('baliza.main.fetch_data_from_pncp')
def test_complete_data_pipeline(mock_fetch, mock_upload, temp_workspace, mock_pncp_response):
    """Test complete data pipeline from harvesting to upload."""
    # Initialize database
    _init_baliza_db()
    
    # Mock PNCP API response
    mock_fetch.return_value = mock_pncp_response
    
    # Mock IA upload success
    mock_upload.return_value = True
    
    # Set environment variables for IA
    os.environ["IA_ACCESS_KEY"] = "test_key"
    os.environ["IA_SECRET_KEY"] = "test_secret"
    
    endpoint_key = "contratos_publicacao"
    endpoint_cfg = ENDPOINTS_CONFIG[endpoint_key]
    test_date = "2024-01-15"
    run_summary = {"contratos_publicacao": {"status": "pending", "files_generated": []}}
    
    # Harvest data
    records = harvest_endpoint_data(test_date, endpoint_key, endpoint_cfg)
    assert len(records) == 2
    
    # Process and upload data
    process_and_upload_data(test_date, endpoint_key, endpoint_cfg, records, run_summary)
    
    # Verify run was logged to database
    conn = duckdb.connect("state/baliza.duckdb")
    runs = conn.execute("SELECT COUNT(*) FROM control.runs").fetchone()
    assert runs[0] >= 1
    
    # Verify PSA data
    psa_records = conn.execute("SELECT COUNT(*) FROM psa.contratos_raw").fetchone()
    assert psa_records[0] >= 2
    
    # Verify Parquet file was created
    parquet_files = [f for f in os.listdir("baliza_data") if f.endswith(".parquet")]
    assert len(parquet_files) >= 1
    
    # Verify upload was called
    mock_upload.assert_called_once()
    
    conn.close()


def test_federation_integration(temp_workspace):
    """Test Internet Archive federation integration."""
    _init_baliza_db()
    
    federation = InternetArchiveFederation("state/baliza.duckdb")
    
    # Test federation initialization
    conn = duckdb.connect("state/baliza.duckdb")
    schemas = conn.execute("SHOW SCHEMAS").fetchall()
    schema_names = [s[0] for s in schemas]
    assert 'federated' in schema_names
    
    conn.close()


@patch('baliza.main.InternetArchiveFederation')
def test_archive_first_data_flow(mock_federation_class, temp_workspace):
    """Test archive-first data flow where IA data is checked first."""
    _init_baliza_db()
    
    # Mock federation instance
    mock_federation = Mock()
    mock_federation_class.return_value = mock_federation
    mock_federation.update_federation.return_value = None
    
    # This should trigger federation update during initialization
    _init_baliza_db()
    
    # Verify federation was initialized
    mock_federation_class.assert_called()


def test_data_quality_checks(temp_workspace, mock_pncp_response):
    """Test data quality validations."""
    _init_baliza_db()
    
    test_records = mock_pncp_response["data"]
    run_id = "quality_test_run"
    data_date = "2024-01-15"
    endpoint_key = "contratos_publicacao"
    
    # Load test data
    _load_to_psa(run_id, data_date, endpoint_key, test_records)
    
    # Verify data quality
    conn = duckdb.connect("state/baliza.duckdb")
    
    # Check for required fields
    null_checks = conn.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(numeroControlePncpCompra) as with_numero_controle,
            COUNT(valorInicial) as with_valor_inicial,
            COUNT(nomeRazaoSocialFornecedor) as with_fornecedor
        FROM psa.contratos_raw 
        WHERE baliza_run_id = ?
    """, [run_id]).fetchone()
    
    assert null_checks[0] == 2  # total records
    assert null_checks[1] == 2  # all have numero controle
    assert null_checks[2] == 2  # all have valor inicial
    assert null_checks[3] == 2  # all have fornecedor
    
    conn.close()


@patch('baliza.main.fetch_data_from_pncp')
def test_multi_page_harvesting(mock_fetch, temp_workspace):
    """Test harvesting data across multiple pages."""
    # Mock multi-page response
    page1_response = {
        "data": [{"numeroControlePncpCompra": "page1-001"}],
        "totalRegistros": 3,
        "totalPaginas": 2,
        "paginaAtual": 1
    }
    
    page2_response = {
        "data": [
            {"numeroControlePncpCompra": "page2-001"},
            {"numeroControlePncpCompra": "page2-002"}
        ],
        "totalRegistros": 3,
        "totalPaginas": 2,
        "paginaAtual": 2
    }
    
    mock_fetch.side_effect = [page1_response, page2_response]
    
    endpoint_key = "contratos_publicacao"
    endpoint_cfg = ENDPOINTS_CONFIG[endpoint_key]
    test_date = "2024-01-15"
    
    records = harvest_endpoint_data(test_date, endpoint_key, endpoint_cfg)
    
    assert len(records) == 3
    assert records[0]["numeroControlePncpCompra"] == "page1-001"
    assert records[1]["numeroControlePncpCompra"] == "page2-001"
    assert records[2]["numeroControlePncpCompra"] == "page2-002"


def test_error_recovery(temp_workspace):
    """Test error recovery and graceful failure handling."""
    _init_baliza_db()
    
    # Test with invalid data
    invalid_records = [{"invalid": "data"}]
    
    # Should handle errors gracefully
    result = _load_to_psa("error_test", "2024-01-15", "test_endpoint", invalid_records)
    
    # Should return 0 for failed insertion but not crash
    assert result >= 0


@patch('os.path.exists')
def test_federation_update_flag(mock_exists, temp_workspace):
    """Test federation update flag prevents unnecessary updates."""
    mock_exists.return_value = True
    
    # Mock file read to return today's date
    with patch('builtins.open', create=True) as mock_open:
        mock_open.return_value.__enter__.return_value.read.return_value = date.today().isoformat()
        
        # Initialize database (should skip federation update)
        _init_baliza_db()
        
        # File should have been checked
        mock_exists.assert_called_with("state/ia_federation_last_update")


def test_monthly_parquet_file_creation(temp_workspace, mock_pncp_response):
    """Test monthly Parquet file creation and appending."""
    _init_baliza_db()
    
    endpoint_key = "contratos_publicacao"
    endpoint_cfg = ENDPOINTS_CONFIG[endpoint_key]
    test_date = "2024-01-15"
    test_records = mock_pncp_response["data"]
    run_summary = {"contratos_publicacao": {"status": "pending", "files_generated": []}}
    
    # Mock IA credentials and upload
    with patch.dict(os.environ, {"IA_ACCESS_KEY": "test", "IA_SECRET_KEY": "test"}), \
         patch('baliza.main.upload'):
        
        process_and_upload_data(test_date, endpoint_key, endpoint_cfg, test_records, run_summary)
    
    # Verify Parquet file was created
    parquet_files = [f for f in os.listdir("baliza_data") if f.endswith(".parquet")]
    assert len(parquet_files) >= 1
    
    # Verify file can be read by DuckDB
    conn = duckdb.connect()
    result = conn.execute(f"SELECT COUNT(*) FROM '{parquet_files[0]}'").fetchone()
    assert result[0] >= 2
    conn.close()


def test_run_logging_and_tracking(temp_workspace, mock_pncp_response):
    """Test run logging and execution tracking."""
    _init_baliza_db()
    
    endpoint_key = "contratos_publicacao"
    endpoint_cfg = ENDPOINTS_CONFIG[endpoint_key]
    test_date = "2024-01-15"
    test_records = mock_pncp_response["data"]
    run_summary = {"contratos_publicacao": {"status": "pending", "files_generated": []}}
    
    # Mock IA credentials and upload
    with patch.dict(os.environ, {"IA_ACCESS_KEY": "test", "IA_SECRET_KEY": "test"}), \
         patch('baliza.main.upload'):
        
        process_and_upload_data(test_date, endpoint_key, endpoint_cfg, test_records, run_summary)
    
    # Verify run was logged
    conn = duckdb.connect("state/baliza.duckdb")
    
    run_record = conn.execute("""
        SELECT run_id, data_date, endpoint_key, records_fetched, upload_status
        FROM control.runs 
        ORDER BY timestamp DESC 
        LIMIT 1
    """).fetchone()
    
    assert run_record[1] == test_date
    assert run_record[2] == endpoint_key
    assert run_record[3] == 2  # records fetched
    assert run_record[4] in ["success", "skipped_no_credentials"]
    
    conn.close()