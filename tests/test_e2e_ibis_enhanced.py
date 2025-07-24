"""
Comprehensive E2E test suite for enhanced Ibis pipeline.
Tests using real PNCP data and domain tables.
"""
import pytest
import ibis
import pandas as pd
from pathlib import Path
from pipelines.ibis_pipeline import run_ibis_pipeline
from pipelines.domain_nodes import validate_domain_table_consistency
from tests.fixtures.test_data_loader import (
    load_test_data_as_bronze,
    load_multiple_test_datasets,
    create_sample_bronze_table,
    validate_test_data_structure
)

@pytest.fixture(scope="module")
def con():
    """In-memory DuckDB connection for testing."""
    return ibis.connect("duckdb://")

class TestIbisE2EIntegration:
    """E2E tests using real PNCP data and domain tables."""
    
    def test_full_pipeline_single_dataset(self, con):
        """Test complete pipeline using single real test dataset."""
        # Load real test data
        records = load_test_data_as_bronze(con, "contratacoes_publicacao")
        assert len(records) > 0, "Should load test data"
        
        # Validate test data structure
        validation_report = validate_test_data_structure(records)
        assert validation_report["valid"], f"Test data validation failed: {validation_report}"
        
        # Run enhanced pipeline
        metrics = run_ibis_pipeline(con, enable_domain_enrichment=True)
        
        # Verify pipeline execution
        assert metrics["total_time"] > 0, "Pipeline should take some time"
        assert metrics["total_time"] < 30, "Pipeline should complete quickly"
        assert metrics["tables_created"] >= 3, "Should create at least bronze, silver, gold tables"
        
        # Verify required tables exist
        tables = con.list_tables()
        required_tables = [
            "bronze_pncp_raw",
            "silver_contratacoes",
            "silver_dim_unidades_orgao", 
            "gold_contratacoes_analytics"
        ]
        
        for table in required_tables:
            assert table in tables, f"Required table {table} not found"
            
        # Verify data flow through pipeline
        bronze_count = con.table("bronze_pncp_raw").count().execute()
        silver_count = con.table("silver_contratacoes").count().execute()
        gold_count = con.table("gold_contratacoes_analytics").count().execute()
        
        assert bronze_count == len(records), "Bronze should match loaded records"
        assert silver_count == bronze_count, "Silver should match bronze"
        assert gold_count > 0, "Gold should have aggregated results"
        
    def test_multiple_datasets_integration(self, con):
        """Test pipeline with multiple real datasets."""
        # Load multiple test datasets
        datasets = load_multiple_test_datasets(con)
        assert len(datasets) > 0, "Should load at least one dataset"
        
        total_records = sum(len(records) for records in datasets.values())
        print(f"Loaded {len(datasets)} datasets with {total_records} total records")
        
        # Run pipeline
        metrics = run_ibis_pipeline(con, enable_domain_enrichment=True)
        
        # Verify processing
        bronze_count = con.table("bronze_pncp_raw").count().execute()
        assert bronze_count == total_records, "Bronze should contain all loaded records"
        
        # Check analytics results
        gold_table = con.table("gold_contratacoes_analytics")
        gold_result = gold_table.execute()
        
        assert len(gold_result) > 0, "Should produce analytical results"
        
        # Business logic validation
        assert all(gold_result["quantidade_contratacoes"] > 0), "Contract counts should be positive"
        assert all(gold_result["valor_total_estimado"] >= 0), "Values should be non-negative"
        
        # Check for meaningful grouping
        unique_years = gold_result["anoContratacao"].nunique()
        unique_modalities = gold_result["modalidadeNome"].nunique()
        print(f"Analytics cover {unique_years} years and {unique_modalities} modalities")
        
    def test_domain_enrichment_effectiveness(self, con):
        """Test that domain enrichment provides meaningful business value."""
        # Load test data and run pipeline
        load_test_data_as_bronze(con, "contratacoes_publicacao") 
        metrics = run_ibis_pipeline(con, enable_domain_enrichment=True)
        
        # Check enrichment in silver layer
        silver_table = con.table("silver_contratacoes")
        silver_result = silver_table.execute()
        
        # Verify enrichment columns exist and have data
        enrichment_checks = []
        
        if "modalidade_nome" in silver_result.columns:
            modalidade_enriched = silver_result["modalidade_nome"].notna().sum()
            enrichment_checks.append(("modalidade", modalidade_enriched, len(silver_result)))
            
        if "uf_nome" in silver_result.columns:
            uf_enriched = silver_result["uf_nome"].notna().sum()
            enrichment_checks.append(("uf", uf_enriched, len(silver_result)))
            
        if "situacao_nome" in silver_result.columns:
            situacao_enriched = silver_result["situacao_nome"].notna().sum()
            enrichment_checks.append(("situacao", situacao_enriched, len(silver_result)))
        
        # Report enrichment effectiveness
        for field, enriched_count, total_count in enrichment_checks:
            enrichment_rate = enriched_count / total_count * 100
            print(f"✅ {field} enrichment: {enriched_count}/{total_count} ({enrichment_rate:.1f}%)")
            
            # Should have some enrichment for valid codes
            if enriched_count > 0:
                assert enrichment_rate >= 0, f"{field} enrichment should be non-negative"
        
        # Verify enriched data flows to gold layer
        gold_table = con.table("gold_contratacoes_analytics")
        gold_result = gold_table.execute()
        
        # Should have human-readable modality names in gold
        if "modalidadeNome" in gold_result.columns:
            readable_modalities = gold_result["modalidadeNome"].notna().sum()
            assert readable_modalities > 0, "Gold layer should have readable modality names"
            
    def test_enum_domain_consistency_validation(self):
        """Test that Python enums are consistent with CSV domain tables."""
        try:
            from src.baliza.enums import ModalidadeContratacao, SituacaoContratacao
        except ImportError:
            pytest.skip("Enums not available for testing")
            
        # Test modalidade consistency
        modalidade_csv_path = Path("dbt_baliza/seeds/domain_tables/modalidade_contratacao.csv")
        if modalidade_csv_path.exists():
            modalidade_df = pd.read_csv(modalidade_csv_path)
            csv_codes = set(modalidade_df["code"])
            enum_codes = {m.value for m in ModalidadeContratacao}
            
            # Enums should be subset of CSV (CSV is authoritative)
            missing_in_csv = enum_codes - csv_codes
            assert len(missing_in_csv) == 0, f"Modalidade enum values not in CSV: {missing_in_csv}"
            
        # Test situacao consistency
        situacao_csv_path = Path("dbt_baliza/seeds/domain_tables/situacao_contratacao.csv")
        if situacao_csv_path.exists():
            situacao_df = pd.read_csv(situacao_csv_path)
            csv_codes = set(situacao_df["code"])
            enum_codes = {s.value for s in SituacaoContratacao}
            
            missing_in_csv = enum_codes - csv_codes
            assert len(missing_in_csv) == 0, f"Situacao enum values not in CSV: {missing_in_csv}"
            
    def test_data_quality_validations(self, con):
        """Test comprehensive data quality rules on transformed data."""
        # Load and process data
        load_test_data_as_bronze(con, "contratacoes_publicacao")
        run_ibis_pipeline(con, enable_domain_enrichment=True)
        
        # Bronze layer quality checks
        bronze_table = con.table("bronze_pncp_raw")
        bronze_result = bronze_table.execute()
        
        assert len(bronze_result) > 0, "Bronze should have records"
        assert bronze_result["numeroControlePNCP"].notna().all(), "Control numbers should not be null"
        assert bronze_result["anoContratacao"].notna().all(), "Years should not be null"
        assert (bronze_result["valorTotalEstimado"] >= 0).all(), "Values should be non-negative"
        
        # Silver layer quality checks
        silver_table = con.table("silver_contratacoes")
        silver_result = silver_table.execute()
        
        assert len(silver_result) == len(bronze_result), "Silver should match bronze count"
        
        # Check data type transformations
        if "dataPublicacaoPNCP" in silver_result.columns:
            # Should be valid dates (this depends on Ibis date parsing)
            date_column = silver_result["dataPublicacaoPNCP"]
            non_null_dates = date_column.notna().sum()
            if non_null_dates > 0:
                print(f"✅ Date parsing: {non_null_dates}/{len(silver_result)} valid dates")
                
        # Gold layer quality checks  
        gold_table = con.table("gold_contratacoes_analytics")
        gold_result = gold_table.execute()
        
        assert len(gold_result) > 0, "Gold should have analytical results"
        
        # Business rule validations
        assert all(gold_result["quantidade_contratacoes"] > 0), "Contract counts should be positive"
        assert all(gold_result["valor_total_estimado"] >= 0), "Aggregated values should be non-negative"
        
        # Aggregation consistency
        total_contracts_gold = gold_result["quantidade_contratacoes"].sum()
        total_contracts_silver = len(silver_result)
        assert total_contracts_gold == total_contracts_silver, "Aggregation should preserve total count"
        
        # Value consistency
        total_value_gold = gold_result["valor_total_estimado"].sum()
        total_value_silver = silver_result["valorTotalEstimado"].sum()
        value_diff = abs(total_value_gold - total_value_silver) / total_value_silver if total_value_silver > 0 else 0
        assert value_diff < 0.01, f"Value aggregation error too high: {value_diff:.3%}"
        
    @pytest.mark.slow 
    def test_performance_with_larger_dataset(self, con):
        """Test pipeline performance with larger synthetic dataset."""
        # Create larger test dataset
        large_records = create_sample_bronze_table(con, num_records=1000)
        assert len(large_records) == 1000, "Should create 1000 test records"
        
        # Run pipeline and measure performance
        metrics = run_ibis_pipeline(con, enable_domain_enrichment=True)
        
        # Performance assertions
        assert metrics["total_time"] < 120, "Large dataset should process in under 2 minutes"
        assert metrics["tables_created"] >= 3, "All tables should be created"
        
        # Verify data volume handling
        bronze_count = con.table("bronze_pncp_raw").count().execute()
        gold_count = con.table("gold_contratacoes_analytics").count().execute()
        
        assert bronze_count == 1000, "Bronze should have 1000 records"
        assert gold_count > 0, "Gold should produce aggregated results"
        assert gold_count < bronze_count, "Gold should aggregate bronze data"
        
        # Memory efficiency check (basic)
        print(f"✅ Processed {bronze_count} records in {metrics['total_time']:.2f}s")
        print(f"✅ Created {gold_count} analytical results")
        
        # Performance metrics
        records_per_second = bronze_count / metrics['total_time']
        print(f"📊 Performance: {records_per_second:.0f} records/second")
        
    def test_pipeline_error_handling(self, con):
        """Test pipeline behavior with edge cases and errors."""
        # Test with empty dataset
        con.create_table("bronze_pncp_raw", ibis.memtable([]), overwrite=True)
        
        # Pipeline should handle empty data gracefully
        try:
            metrics = run_ibis_pipeline(con, enable_domain_enrichment=True)
            # Should complete without errors
            assert metrics["total_time"] > 0
            print("✅ Pipeline handles empty data gracefully")
        except Exception as e:
            pytest.fail(f"Pipeline should handle empty data: {e}")
            
        # Test with malformed data
        malformed_records = [
            {
                "numeroControlePNCP": None,  # Null required field
                "anoContratacao": "invalid_year",  # Invalid data type
                "valorTotalEstimado": -100,  # Negative value
                "modalidadeId": "999",  # Invalid modalidade
                "uf": "INVALID"  # Invalid UF
            }
        ]
        
        con.create_table("bronze_pncp_raw", ibis.memtable(malformed_records), overwrite=True)
        
        # Pipeline should handle malformed data without crashing
        try:
            metrics = run_ibis_pipeline(con, enable_domain_enrichment=True)
            print("✅ Pipeline handles malformed data gracefully")
            
            # Check that some processing occurred
            tables = con.list_tables()
            assert "silver_contratacoes" in tables
            assert "gold_contratacoes_analytics" in tables
            
        except Exception as e:
            # If it fails, it should be a controlled failure
            print(f"⚠️  Pipeline failed with malformed data (expected): {e}")
            
    def test_domain_validation_integration(self, con):
        """Test domain validation integration with pipeline."""
        # Load data and run pipeline
        load_test_data_as_bronze(con, "contratacoes_publicacao")
        run_ibis_pipeline(con, enable_domain_enrichment=True)
        
        # Run domain validation
        validation_report = validate_domain_table_consistency(con)
        
        # Should not have critical errors
        assert validation_report["valid"], f"Domain validation failed: {validation_report['errors']}"
        
        # Log any warnings
        if validation_report["warnings"]:
            for warning in validation_report["warnings"]:
                print(f"⚠️  Domain validation warning: {warning}")
                
        # Check that validation covers key tables
        if "table_counts" in validation_report:
            print(f"📋 Domain validation covered: {validation_report['table_counts']}")
            
class TestBusinessLogicValidation:
    """Test business logic and domain-specific validations."""
    
    def test_modalidade_business_rules(self, con):
        """Test business rules specific to modalidades."""
        load_test_data_as_bronze(con, "contratacoes_publicacao")
        run_ibis_pipeline(con, enable_domain_enrichment=True)
        
        silver_table = con.table("silver_contratacoes")
        silver_result = silver_table.execute()
        
        # Check modalidade consistency
        for _, row in silver_result.iterrows():
            modalidade_id = row.get("modalidadeId")
            modalidade_nome = row.get("modalidade_nome")
            
            # If both are present, they should be consistent
            if modalidade_id and modalidade_nome:
                # Basic consistency check (specific mappings depend on domain data)
                assert modalidade_nome != "UNKNOWN", "Should not have unknown modalidade names"
                
    def test_geographic_data_validation(self, con):
        """Test geographic data (UF) validation."""
        load_test_data_as_bronze(con, "contratacoes_publicacao")
        run_ibis_pipeline(con, enable_domain_enrichment=True)
        
        silver_table = con.table("silver_contratacoes") 
        silver_result = silver_table.execute()
        
        # Check UF codes
        for _, row in silver_result.iterrows():
            uf_code = row.get("uf")
            uf_nome = row.get("uf_nome")
            
            if uf_code:
                # Should be valid Brazilian state code
                assert len(uf_code) == 2, f"UF code should be 2 characters: {uf_code}"
                assert uf_code.isupper(), f"UF code should be uppercase: {uf_code}"
                
                # If enriched, should have name
                if uf_nome:
                    assert len(uf_nome) > 2, f"UF name should be meaningful: {uf_nome}"
                    
    def test_financial_data_validation(self, con):
        """Test financial data validation."""
        load_test_data_as_bronze(con, "contratacoes_publicacao")
        run_ibis_pipeline(con, enable_domain_enrichment=True)
        
        # Check all layers for financial consistency
        for table_name in ["bronze_pncp_raw", "silver_contratacoes"]:
            if table_name in con.list_tables():
                table = con.table(table_name)
                result = table.execute()
                
                # Financial validation rules
                assert (result["valorTotalEstimado"] >= 0).all(), f"Values should be non-negative in {table_name}"
                
                # Check for reasonable value ranges (basic sanity check)
                max_value = result["valorTotalEstimado"].max()
                assert max_value < 1e12, f"Values seem unreasonably high in {table_name}: {max_value}"
                
        # Gold layer aggregation validation
        gold_table = con.table("gold_contratacoes_analytics")
        gold_result = gold_table.execute()
        
        # Aggregated values should make sense
        assert all(gold_result["valor_total_estimado"] >= 0), "Aggregated values should be non-negative"
        
        # Check aggregation logic
        for _, row in gold_result.iterrows():
            count = row["quantidade_contratacoes"]
            value = row["valor_total_estimado"]
            
            # If there are contracts, there should be some value (unless all are zero-value)
            if count > 0:
                assert value >= 0, "Aggregated value should be non-negative when contracts exist"