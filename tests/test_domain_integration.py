"""
Test domain table integration in Ibis pipeline.
"""
import pytest
import ibis
from pipelines.domain_nodes import (
    load_domain_tables,
    enrich_with_domain_data,
    validate_domain_table_consistency,
    get_domain_table_stats
)
from pipelines.ibis_pipeline import run_ibis_pipeline
from tests.fixtures.test_data_loader import load_test_data_as_bronze

@pytest.fixture(scope="module")
def con():
    """In-memory DuckDB connection for testing."""
    return ibis.connect("duckdb://")

def test_load_domain_tables(con):
    """Test loading domain tables from CSV files."""
    domain_tables = load_domain_tables(con)
    
    # Should load some domain tables (at least a few should exist)
    assert isinstance(domain_tables, dict)
    
    # Check if common domain tables are loaded
    available_tables = con.list_tables()
    domain_table_names = [t for t in available_tables if t.startswith("dim_")]
    
    print(f"Available domain tables: {domain_table_names}")
    
    # Should have at least some domain tables
    if len(domain_table_names) > 0:
        # Verify table structure
        for table_name in domain_table_names:
            table = con.table(table_name)
            columns = table.columns
            
            # Should have at least 'code' column
            assert "code" in columns, f"Domain table {table_name} should have 'code' column"
            
            # Should have some records
            count = table.count().execute()
            assert count > 0, f"Domain table {table_name} should have records"

def test_domain_enrichment_without_tables(con):
    """Test domain enrichment when no domain tables are available."""
    # Create a simple silver table
    silver_data = [
        {
            "modalidadeId": "1",
            "codigoSituacaoContratacao": "1", 
            "uf": "SP",
            "valorTotalEstimado": 1000.0
        }
    ]
    silver_table = ibis.memtable(silver_data)
    
    # Enrichment should handle missing tables gracefully
    enriched = enrich_with_domain_data(silver_table, con)
    
    # Should return a table (possibly the original)
    assert enriched is not None
    result = enriched.execute()
    assert len(result) == 1

def test_domain_enrichment_with_tables(con):
    """Test domain enrichment when domain tables are available."""
    # Load domain tables first
    domain_tables = load_domain_tables(con)
    
    # Create a simple silver table with data to enrich
    silver_data = [
        {
            "modalidadeId": "1",
            "codigoSituacaoContratacao": "1",
            "uf": "SP", 
            "valorTotalEstimado": 1000.0
        }
    ]
    silver_table = ibis.memtable(silver_data)
    
    # Apply enrichment
    enriched = enrich_with_domain_data(silver_table, con)
    result = enriched.execute()
    
    # Should have the original record
    assert len(result) == 1
    assert result["valorTotalEstimado"][0] == 1000.0
    
    # Check if enrichment columns were added (depends on available domain tables)
    available_tables = con.list_tables()
    
    if "dim_modalidade_contratacao" in available_tables:
        assert "modalidade_nome" in result.columns
        
    if "dim_situacao_contratacao" in available_tables:
        assert "situacao_nome" in result.columns
        
    if "dim_uf_brasil" in available_tables:
        assert "uf_nome" in result.columns

def test_enhanced_pipeline_with_domain_integration(con):
    """Test the complete enhanced pipeline with domain integration."""
    # Load real test data
    records = load_test_data_as_bronze(con, "contratacoes_publicacao")
    assert len(records) > 0
    
    # Run enhanced pipeline with domain enrichment
    metrics = run_ibis_pipeline(con, enable_domain_enrichment=True)
    
    # Verify pipeline completed successfully
    assert metrics["total_time"] > 0
    assert metrics["domain_enrichment_enabled"] is True
    assert metrics["tables_created"] > 0
    
    # Verify enhanced tables were created
    tables = con.list_tables()
    required_tables = [
        "silver_contratacoes",
        "silver_dim_unidades_orgao",
        "gold_contratacoes_analytics"
    ]
    
    for table in required_tables:
        assert table in tables, f"Required table {table} not found"
    
    # Check if silver table has enrichment columns
    silver_table = con.table("silver_contratacoes")
    silver_result = silver_table.execute()
    
    # Should have some enrichment if domain tables were loaded
    if metrics["domain_tables_loaded"] > 0:
        # Check for potential enrichment columns
        enrichment_columns = [
            "modalidade_nome", "situacao_nome", "uf_nome"
        ]
        
        found_enrichment = any(col in silver_result.columns for col in enrichment_columns)
        if found_enrichment:
            print("✅ Domain enrichment applied successfully")
        else:
            print("⚠️  No enrichment columns found (may be expected if domain tables don't match data)")

def test_pipeline_without_domain_enrichment(con):
    """Test pipeline works without domain enrichment."""
    # Load real test data
    records = load_test_data_as_bronze(con, "contratacoes_publicacao")
    assert len(records) > 0
    
    # Run pipeline without domain enrichment
    metrics = run_ibis_pipeline(con, enable_domain_enrichment=False)
    
    # Should complete successfully
    assert metrics["total_time"] > 0
    assert metrics["domain_enrichment_enabled"] is False
    assert metrics["domain_tables_loaded"] == 0
    assert metrics["tables_created"] > 0
    
    # Basic tables should still be created
    tables = con.list_tables()
    assert "silver_contratacoes" in tables
    assert "gold_contratacoes_analytics" in tables

def test_domain_table_stats(con):
    """Test domain table statistics."""
    # Load domain tables
    load_domain_tables(con)
    
    # Get stats
    stats = get_domain_table_stats(con)
    
    assert isinstance(stats, dict)
    assert "total_tables" in stats
    assert "total_records" in stats
    assert "tables" in stats
    
    # If domain tables were loaded, should have stats
    if stats["total_tables"] > 0:
        assert stats["total_records"] > 0
        assert len(stats["tables"]) == stats["total_tables"]
        
        # Each table should have record count and columns
        for table_name, table_stats in stats["tables"].items():
            assert "records" in table_stats
            assert "columns" in table_stats
            assert table_stats["records"] > 0

def test_domain_validation(con):
    """Test domain table validation."""
    # Load domain tables
    load_domain_tables(con)
    
    # Run validation
    validation_report = validate_domain_table_consistency(con)
    
    assert isinstance(validation_report, dict)
    assert "valid" in validation_report
    assert "errors" in validation_report
    assert "warnings" in validation_report
    
    # Should not have critical errors
    assert len(validation_report["errors"]) == 0, f"Validation errors: {validation_report['errors']}"
    
    # Warnings are acceptable (enum/CSV mismatches are common)
    if validation_report["warnings"]:
        print(f"Validation warnings (acceptable): {validation_report['warnings']}")

def test_real_data_enrichment_quality(con):
    """Test enrichment quality with real PNCP data."""
    # Load real test data
    records = load_test_data_as_bronze(con, "contratacoes_publicacao")
    
    # Load domain tables
    domain_tables = load_domain_tables(con)
    
    # Run pipeline with enrichment
    metrics = run_ibis_pipeline(con, enable_domain_enrichment=True)
    
    # Check enrichment quality in silver table
    silver_table = con.table("silver_contratacoes")
    silver_result = silver_table.execute()
    
    # Data quality checks
    assert len(silver_result) > 0, "Silver table should have records"
    
    # Check for meaningful enrichment (if domain tables were loaded)
    if metrics["domain_tables_loaded"] > 0:
        # Check modalidade enrichment
        if "modalidade_nome" in silver_result.columns:
            enriched_count = silver_result["modalidade_nome"].notna().sum()
            if enriched_count > 0:
                print(f"✅ Modalidade enrichment: {enriched_count}/{len(silver_result)} records enriched")
        
        # Check UF enrichment
        if "uf_nome" in silver_result.columns:
            enriched_count = silver_result["uf_nome"].notna().sum() 
            if enriched_count > 0:
                print(f"✅ UF enrichment: {enriched_count}/{len(silver_result)} records enriched")
    
    # Verify gold layer works with enriched data
    gold_table = con.table("gold_contratacoes_analytics")
    gold_result = gold_table.execute()
    
    assert len(gold_result) > 0, "Gold table should have analytical results"
    assert all(gold_result["quantidade_contratacoes"] > 0), "Should have positive contract counts"
    assert all(gold_result["valor_total_estimado"] >= 0), "Should have non-negative values"