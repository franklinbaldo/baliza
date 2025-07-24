"""
Test the real data loader functionality.
"""
import pytest
import ibis
from tests.fixtures.test_data_loader import (
    load_test_data_as_bronze,
    load_multiple_test_datasets,
    validate_test_data_structure
)

@pytest.fixture(scope="module")
def con():
    """In-memory DuckDB connection for testing."""
    return ibis.connect("duckdb://")

def test_load_single_test_dataset(con):
    """Test loading a single test dataset."""
    records = load_test_data_as_bronze(con, "contratacoes_publicacao")
    
    # Should have loaded records
    assert len(records) > 0, "Should load at least some records"
    assert len(records) <= 10, "Test data should have max 10 records per file"
    
    # Verify bronze table was created
    assert "bronze_pncp_raw" in con.list_tables()
    
    # Verify table has correct number of records
    bronze_table = con.table("bronze_pncp_raw")
    table_count = bronze_table.count().execute()
    assert table_count == len(records)

def test_validate_data_structure(con):
    """Test data structure validation."""
    records = load_test_data_as_bronze(con, "contratacoes_publicacao")
    validation_report = validate_test_data_structure(records)
    
    # Should be valid
    assert validation_report["valid"] is True
    assert validation_report["total_records"] > 0
    
    # Check data quality metrics
    quality = validation_report["data_quality"]
    assert quality["unique_contracts"] > 0
    assert quality["year_range"]["min"] >= 2020  # Reasonable year range
    assert quality["year_range"]["max"] <= 2030
    assert quality["value_range"]["min"] >= 0  # Non-negative values
    
def test_required_fields_present(con):
    """Test that all required fields are present in loaded data."""
    records = load_test_data_as_bronze(con, "contratacoes_publicacao")
    
    required_fields = [
        "numeroControlePNCP", "anoContratacao", "modalidadeId",
        "cnpjOrgao", "uf", "valorTotalEstimado"
    ]
    
    for record in records:
        for field in required_fields:
            assert field in record, f"Required field {field} missing from record"
            
def test_multiple_datasets_loading(con):
    """Test loading multiple test datasets."""
    datasets = load_multiple_test_datasets(con)
    
    # Should load at least one dataset
    assert len(datasets) > 0, "Should load at least one test dataset"
    
    # Verify bronze table has combined data
    bronze_table = con.table("bronze_pncp_raw")
    table_count = bronze_table.count().execute()
    
    total_records = sum(len(records) for records in datasets.values())
    assert table_count == total_records, "Bronze table should contain all loaded records"

def test_data_types_compatibility(con):
    """Test that loaded data types are compatible with Ibis pipeline."""
    records = load_test_data_as_bronze(con, "contratacoes_publicacao")
    
    for record in records:
        # Test critical data type conversions
        assert isinstance(record["anoContratacao"], int), "Year should be integer"
        assert isinstance(record["valorTotalEstimado"], float), "Value should be float"
        assert isinstance(record["numeroControlePNCP"], str), "Control number should be string"
        assert isinstance(record["modalidadeId"], str), "Modalidade ID should be string"
        
def test_pipeline_integration(con):
    """Test that real data works with the Ibis pipeline."""
    from pipelines.ibis_pipeline import run_ibis_pipeline
    
    # Load real test data
    records = load_test_data_as_bronze(con, "contratacoes_publicacao")
    assert len(records) > 0
    
    # Run the pipeline with real data
    run_ibis_pipeline(con)
    
    # Verify pipeline completed successfully
    required_tables = [
        "silver_contratacoes",
        "silver_dim_unidades_orgao", 
        "gold_contratacoes_analytics"
    ]
    
    tables = con.list_tables()
    for table in required_tables:
        assert table in tables, f"Pipeline should create {table}"
        
    # Verify data made it through the pipeline
    gold_table = con.table("gold_contratacoes_analytics")
    gold_count = gold_table.count().execute()
    assert gold_count > 0, "Gold table should have analytical results"
    
    # Verify data quality in gold layer
    result = gold_table.execute()
    assert all(result["quantidade_contratacoes"] > 0), "Contract counts should be positive"
    assert all(result["valor_total_estimado"] >= 0), "Values should be non-negative"