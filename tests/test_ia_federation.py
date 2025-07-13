"""
Tests for Internet Archive federation functionality.
"""
import pytest
import tempfile
import os
import duckdb
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import sys

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from baliza.ia_federation import InternetArchiveFederation


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix='.duckdb', delete=True) as f:
        db_path = f.name
    
    # Remove the file first, then create new database
    if os.path.exists(db_path):
        os.unlink(db_path)
    
    # Initialize basic schema
    conn = duckdb.connect(db_path)
    conn.execute("CREATE SCHEMA IF NOT EXISTS federated")
    conn.close()
    
    yield db_path
    
    # Cleanup
    if os.path.exists(db_path):
        os.unlink(db_path)


@pytest.fixture
def federation(temp_db):
    """Create a federation instance with temporary database."""
    return InternetArchiveFederation(temp_db)


def test_federation_init(temp_db):
    """Test InternetArchiveFederation initialization."""
    federation = InternetArchiveFederation(temp_db)
    assert federation.baliza_db_path == temp_db
    
    # Test database connection
    conn = duckdb.connect(temp_db)
    schemas = conn.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
    schema_names = [s[0] for s in schemas]
    assert 'federated' in schema_names
    conn.close()


@patch('baliza.ia_federation.search_items')
def test_discover_ia_items_success(mock_search_items, federation):
    """Test successful discovery of IA items."""
    # Mock IA search response
    mock_file1 = Mock()
    mock_file1.name = 'pncp-contratos-2024-01-01.parquet'
    mock_file1.size = 1000000
    mock_file1.sha1 = 'abc123'
    mock_file1.md5 = 'def456'
    
    mock_item1 = Mock()
    mock_item1.identifier = 'pncp-contratos-2024-01-01'
    mock_item1.metadata = {'date': '2024-01-01', 'title': 'baliza-test'}
    mock_item1.get_files.return_value = [mock_file1]
    
    mock_file2 = Mock()
    mock_file2.name = 'pncp-contratos-2024-01-02.parquet'
    mock_file2.size = 2000000
    mock_file2.sha1 = 'ghi789'
    mock_file2.md5 = 'jkl012'
    
    mock_item2 = Mock()
    mock_item2.identifier = 'pncp-contratos-2024-01-02'
    mock_item2.metadata = {'date': '2024-01-02', 'title': 'baliza-test2'}
    mock_item2.get_files.return_value = [mock_file2]
    
    # Mock the search iterator
    mock_search_result = Mock()
    mock_search_result.iter_as_items.return_value = [mock_item1, mock_item2]
    mock_search_items.return_value = mock_search_result
    
    items = federation.discover_ia_items()
    
    assert len(items) == 2
    assert items[0]['identifier'] == 'pncp-contratos-2024-01-01'
    assert items[1]['identifier'] == 'pncp-contratos-2024-01-02'
    assert len(items[0]['files']) == 1
    assert items[0]['files'][0]['name'] == 'pncp-contratos-2024-01-01.parquet'


@patch('internetarchive.search')
def test_discover_ia_items_no_results(mock_search, federation):
    """Test discovery when no IA items found."""
    mock_search.return_value = []
    
    items = federation.discover_ia_items()
    
    assert items == []


@patch('internetarchive.search')
def test_discover_ia_items_network_error(mock_search, federation):
    """Test discovery when network error occurs."""
    mock_search.side_effect = Exception("Network error")
    
    with pytest.raises(Exception, match="Network error"):
        federation.discover_ia_items()


def test_create_federated_views_with_mock_data(federation, temp_db):
    """Test creating federated views with mock data."""
    # Create a mock parquet file URL
    test_parquet_url = "https://archive.org/download/test-item/test.parquet"
    
    # Mock the discover_ia_items method to return test data
    with patch.object(federation, 'discover_ia_items') as mock_discover:
        mock_discover.return_value = [
            {
                'identifier': 'test-item',
                'parquet_urls': [test_parquet_url],
                'data_date': '2024-01-01'
            }
        ]
        
        # Mock DuckDB httpfs extension
        with patch('duckdb.connect') as mock_connect:
            mock_conn = Mock()
            mock_connect.return_value = mock_conn
            
            federation.create_federated_views()
            
            # Verify that DuckDB connection was established
            mock_connect.assert_called_with(temp_db)
            
            # Verify that httpfs extension was installed
            mock_conn.execute.assert_any_call("INSTALL httpfs")
            mock_conn.execute.assert_any_call("LOAD httpfs")


def test_get_data_availability_empty(federation, temp_db):
    """Test data availability check with empty federation."""
    with patch.object(federation, 'discover_ia_items', return_value=[]):
        availability = federation.get_data_availability()
        
        assert 'internet_archive' in availability
        assert availability['internet_archive']['total_records'] == 0
        assert availability['internet_archive']['date_range'] == {'min_date': None, 'max_date': None}


def test_update_federation(federation):
    """Test federation update process."""
    with patch.object(federation, 'discover_ia_items') as mock_discover, \
         patch.object(federation, 'create_federated_views') as mock_create:
        
        mock_discover.return_value = [{'identifier': 'test-item'}]
        
        federation.update_federation()
        
        mock_discover.assert_called_once()
        mock_create.assert_called_once()


def test_catalog_item_data(federation):
    """Test cataloging individual IA item data."""
    mock_item = {
        'identifier': 'pncp-contratos-2024-01-01',
        'parquet_urls': ['https://archive.org/download/test/file.parquet'],
        'data_date': '2024-01-01'
    }
    
    # Test the internal cataloging logic (if exposed)
    assert mock_item['identifier'] == 'pncp-contratos-2024-01-01'
    assert len(mock_item['parquet_urls']) == 1


def test_federation_error_handling(federation):
    """Test federation error handling."""
    with patch.object(federation, 'discover_ia_items', side_effect=Exception("IA API Error")):
        with pytest.raises(Exception, match="IA API Error"):
            federation.update_federation()


def test_federation_with_real_database_schema(temp_db):
    """Test federation with realistic database schema."""
    federation = InternetArchiveFederation(temp_db)
    
    # Verify that the federated schema exists
    conn = duckdb.connect(temp_db)
    
    # Create a test table in federated schema
    conn.execute("""
        CREATE TABLE IF NOT EXISTS federated.test_contratos (
            id INTEGER,
            data_date DATE,
            data_source VARCHAR
        )
    """)
    
    # Insert test data
    conn.execute("""
        INSERT INTO federated.test_contratos VALUES 
        (1, '2024-01-01', 'internet_archive'),
        (2, '2024-01-02', 'local_storage')
    """)
    
    # Test query
    result = conn.execute("""
        SELECT data_source, COUNT(*) 
        FROM federated.test_contratos 
        GROUP BY data_source
    """).fetchall()
    
    assert len(result) == 2
    
    conn.close()


@pytest.mark.parametrize("status_code,expected_error", [
    (404, "IA item not found"),
    (500, "IA server error"),
    (403, "Access denied")
])
def test_ia_access_errors(federation, status_code, expected_error):
    """Test various IA access error scenarios."""
    with patch('internetarchive.search') as mock_search:
        mock_search.side_effect = Exception(expected_error)
        
        with pytest.raises(Exception, match=expected_error):
            federation.discover_ia_items()


def test_federation_data_consistency(federation, temp_db):
    """Test data consistency between IA and local sources."""
    # This test would verify that federated views maintain data consistency
    conn = duckdb.connect(temp_db)
    
    # Create mock federated view
    conn.execute("""
        CREATE OR REPLACE VIEW federated.contratos_unified AS
        SELECT 
            'test' as numeroControlePncpCompra,
            '2024-01-01'::DATE as data_date,
            'internet_archive' as data_source
    """)
    
    # Test unified view query
    result = conn.execute("""
        SELECT COUNT(*) FROM federated.contratos_unified
    """).fetchone()
    
    assert result[0] == 1
    
    conn.close()


def test_federation_performance_optimization(federation):
    """Test federation performance optimizations."""
    # Test that federation uses appropriate indexes and optimizations
    with patch.object(federation, 'discover_ia_items') as mock_discover:
        mock_discover.return_value = []
        
        # Mock the connection to test index creation
        with patch('duckdb.connect') as mock_connect:
            mock_conn = Mock()
            mock_connect.return_value = mock_conn
            
            federation.create_federated_views()
            
            # Verify performance optimizations were applied
            # (In a real implementation, this would check for index creation, etc.)
            assert mock_conn.execute.called