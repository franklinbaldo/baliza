# Ibis Pipeline Enhancement Plan

**Branch**: `origin/feature/ibis-pipeline`  
**Analysis Date**: July 24, 2025  
**Status**: Review Complete - Implementation Recommendations  

## Executive Summary

The `origin/feature/ibis-pipeline` branch introduces a significant architectural improvement by replacing dbt with Ibis for data transformation. This document provides a comprehensive analysis and enhancement recommendations to leverage existing test data and domain tables for a more robust implementation.

## Current Branch Analysis

### ✅ Strengths
- **Clean Architecture**: Well-structured pipeline with clear separation between Silver and Gold layers
- **Type Safety**: Proper Ibis type hints throughout the codebase
- **CLI Integration**: Backward-compatible implementation with `--dbt` flag support
- **Testing Foundation**: Basic E2E test structure in place

### ⚠️ Critical Issues Identified

1. **Missing Import**: `transform_silver_dim_unidades_orgao` function called but not imported
2. **Hardcoded Test Data**: Uses inline mock data instead of real PNCP test files
3. **No Domain Integration**: Disconnected from existing CSV domain tables and enums
4. **Limited Test Coverage**: Only basic pipeline execution tested

## Available Assets

### Test Data (`tests/test_data/`)
- **6 PNCP endpoints** with real API response samples
- **10 records per endpoint** covering major data types
- **July 2024 data** with proper metadata tracking
- **Structured JSON format** ready for pipeline testing

### Domain Tables (`dbt_baliza/seeds/domain_tables/`)
- **14 official reference tables** from PNCP Manual v1
- **Standardized CSV format**: `code,name/description`
- **Complete PNCP enumeration coverage**
- **Well-documented** with manual cross-references

### Enum System (`src/baliza/enums.py`)
- **16 comprehensive Python enums** (310 total values)
- **Validation utilities** and conversion functions
- **Registry system** for dynamic access
- **Human-readable descriptions** for UI/reporting

## Enhancement Recommendations

### 1. Fix Critical Import Issue ⚡ Priority: IMMEDIATE

**File**: `pipelines/ibis_pipeline.py:2`

```python
# BEFORE
from pipelines.ibis_nodes import (
    transform_silver_contratacoes,
    create_gold_contratacoes_analytics,
)

# AFTER  
from pipelines.ibis_nodes import (
    transform_silver_contratacoes,
    transform_silver_dim_unidades_orgao,  # ADD THIS
    create_gold_contratacoes_analytics,
)
```

### 2. Integrate Real Test Data 🧪 Priority: HIGH

Replace hardcoded mock data with real PNCP test files:

**Create**: `tests/fixtures/test_data_loader.py`

```python
import json
import ibis
from pathlib import Path

def load_test_data_as_bronze(con: ibis.BaseBackend, data_type: str = "contratacoes_publicacao"):
    """Load real PNCP test data into bronze tables."""
    test_file = Path(f"tests/test_data/{data_type}.json")
    
    with open(test_file) as f:
        test_data = json.load(f)
    
    # Transform API response to bronze table format
    records = []
    for item in test_data["data"]["data"]:
        records.append({
            "numeroControlePNCP": item["numeroControlePNCP"],
            "anoContratacao": item.get("anoCompra", item.get("anoContratacao")),
            "dataPublicacaoPNCP": item["dataPublicacaoPncp"],
            "modalidadeId": item["modalidadeId"], 
            "cnpjOrgao": item["orgaoEntidade"]["cnpj"],
            "uf": item["unidadeOrgao"]["ufSigla"],
            "valorTotalEstimado": item.get("valorTotalEstimado", 0),
            # Map remaining fields based on API structure
        })
    
    con.create_table("bronze_pncp_raw", ibis.memtable(records), overwrite=True)
    return records
```

**Update**: `tests/test_ibis_pipeline.py`

```python
# REPLACE hardcoded setup_data fixture
@pytest.fixture(scope="module")
def setup_data(con):
    """Load real test data from tests/test_data/"""
    from tests.fixtures.test_data_loader import load_test_data_as_bronze
    return load_test_data_as_bronze(con, "contratacoes_publicacao")
```

### 3. Domain Table Integration System 🗂️ Priority: HIGH

**Create**: `pipelines/domain_nodes.py`

```python
import ibis
import pandas as pd
from pathlib import Path
from src.baliza.ui import get_console

def load_domain_tables(con: ibis.BaseBackend) -> dict[str, ibis.Table]:
    """Load all CSV domain tables as Ibis tables for reference joins."""
    console = get_console()
    domain_path = Path("dbt_baliza/seeds/domain_tables")
    domain_tables = {}
    
    csv_files = [f for f in domain_path.glob("*.csv") if f.name != "README.md"]
    console.print(f"📋 Loading {len(csv_files)} domain tables...")
    
    for csv_file in csv_files:
        table_name = f"dim_{csv_file.stem}"
        df = pd.read_csv(csv_file)
        
        # Standardize column names
        if "name" not in df.columns and "description" in df.columns:
            df = df.rename(columns={"description": "name"})
            
        domain_tables[table_name] = con.create_table(
            table_name, ibis.memtable(df), overwrite=True
        )
        console.print(f"  ✅ {table_name}: {len(df)} records")
    
    return domain_tables

def enrich_with_domain_data(silver_table: ibis.Table, con: ibis.BaseBackend) -> ibis.Table:
    """Enrich silver tables with human-readable domain descriptions."""
    
    # Available domain tables
    try:
        modalidade_dim = con.table("dim_modalidade_contratacao")
        situacao_dim = con.table("dim_situacao_contratacao") 
        uf_dim = con.table("dim_uf_brasil")
    except Exception:
        # Graceful fallback if domain tables not loaded
        return silver_table
    
    # Progressive enrichment with left joins
    enriched = silver_table
    
    # Add modalidade names
    if "modalidadeId" in silver_table.columns:
        enriched = enriched.join(
            modalidade_dim, 
            enriched.modalidadeId == modalidade_dim.code, 
            how="left"
        ).select(
            enriched,
            modalidade_dim.name.name("modalidade_nome")
        )
    
    # Add situacao names  
    if "codigoSituacaoContratacao" in enriched.columns:
        enriched = enriched.join(
            situacao_dim,
            enriched.codigoSituacaoContratacao == situacao_dim.code,
            how="left"
        ).select(
            enriched,
            situacao_dim.name.name("situacao_nome")
        )
    
    # Add UF names
    if "uf" in enriched.columns:
        enriched = enriched.join(
            uf_dim,
            enriched.uf == uf_dim.code,
            how="left"
        ).select(
            enriched, 
            uf_dim.name.name("uf_nome")
        )
    
    return enriched
```

**Update**: `pipelines/ibis_pipeline.py`

```python
import ibis
from pipelines.ibis_nodes import (
    transform_silver_contratacoes,
    transform_silver_dim_unidades_orgao,  # FIXED IMPORT
    create_gold_contratacoes_analytics,
)
from pipelines.domain_nodes import load_domain_tables, enrich_with_domain_data
from src.baliza.ui import get_console
import time

def run_ibis_pipeline(con: ibis.BaseBackend) -> dict:
    """Enhanced Ibis pipeline with domain integration and monitoring."""
    console = get_console()
    start_time = time.time()
    step_times = {}
    
    console.print("🦜 [bold blue]Starting Enhanced Ibis Pipeline[/bold blue]")
    
    # 1. Load domain reference tables
    step_start = time.time()
    domain_tables = load_domain_tables(con)
    step_times["domain_loading"] = time.time() - step_start
    console.print(f"✅ Domain tables loaded ({step_times['domain_loading']:.2f}s)")
    
    # 2. Silver layer transformations
    step_start = time.time()
    silver_contratacoes = transform_silver_contratacoes(con)
    silver_contratacoes_enriched = enrich_with_domain_data(silver_contratacoes, con)
    con.create_table("silver_contratacoes", silver_contratacoes_enriched, overwrite=True)
    
    silver_dim_unidades_orgao = transform_silver_dim_unidades_orgao(con)
    con.create_table("silver_dim_unidades_orgao", silver_dim_unidades_orgao, overwrite=True)
    step_times["silver_layer"] = time.time() - step_start
    console.print(f"✅ Silver layer complete ({step_times['silver_layer']:.2f}s)")
    
    # 3. Gold layer analytics
    step_start = time.time()
    gold_contratacoes_analytics = create_gold_contratacoes_analytics(
        silver_contratacoes_enriched, silver_dim_unidades_orgao
    )
    con.create_table("gold_contratacoes_analytics", gold_contratacoes_analytics, overwrite=True)
    step_times["gold_layer"] = time.time() - step_start
    console.print(f"✅ Gold layer complete ({step_times['gold_layer']:.2f}s)")
    
    total_time = time.time() - start_time
    console.print(f"🎉 [bold green]Pipeline completed in {total_time:.2f}s[/bold green]")
    
    return {
        "total_time": total_time,
        "step_times": step_times,
        "tables_created": len(con.list_tables()),
        "domain_tables_loaded": len(domain_tables)
    }

if __name__ == "__main__":
    con = ibis.connect("duckdb://baliza.db")
    metrics = run_ibis_pipeline(con)
    print(f"Ibis pipeline executed successfully: {metrics}")
```

### 4. Comprehensive E2E Test Suite 🧪 Priority: MEDIUM

**Create**: `tests/test_e2e_ibis_enhanced.py`

```python
import pytest
import json
from pathlib import Path
import pandas as pd
from pipelines.ibis_pipeline import run_ibis_pipeline
from tests.fixtures.test_data_loader import load_test_data_as_bronze

class TestIbisE2EIntegration:
    """E2E tests using real PNCP data and domain tables."""
    
    def test_full_pipeline_with_real_data(self, con):
        """Test complete pipeline using real test data files."""
        # Load real test data
        records = load_test_data_as_bronze(con, "contratacoes_publicacao")
        assert len(records) == 10  # Should match test data
        
        # Run enhanced pipeline
        metrics = run_ibis_pipeline(con)
        
        # Verify pipeline execution
        assert metrics["total_time"] < 30  # Should be fast
        assert metrics["tables_created"] >= 5  # bronze + silver + gold + domains
        assert metrics["domain_tables_loaded"] >= 10  # Most domain tables
        
        # Verify table creation
        tables = con.list_tables()
        required_tables = [
            "bronze_pncp_raw",
            "silver_contratacoes", 
            "silver_dim_unidades_orgao",
            "gold_contratacoes_analytics",
            "dim_modalidade_contratacao"
        ]
        
        for table in required_tables:
            assert table in tables, f"Required table {table} not found"
            
    def test_domain_enrichment_quality(self, con):
        """Test that domain table joins provide meaningful enrichment."""
        load_test_data_as_bronze(con)
        run_ibis_pipeline(con)
        
        # Check enrichment results
        silver = con.table("silver_contratacoes")
        result = silver.execute()
        
        # Should have enriched columns
        assert "modalidade_nome" in result.columns
        assert "uf_nome" in result.columns
        
        # Should have non-null enrichment for valid codes
        enriched_rows = result[result["modalidade_nome"].notna()]
        assert len(enriched_rows) > 0, "No rows were enriched with modalidade names"
        
        # Verify specific business logic
        leilao_rows = result[result["modalidade_nome"] == "LEILÃO"]
        if len(leilao_rows) > 0:
            assert all(leilao_rows["modalidadeId"] == 3), "Modalidade enrichment mismatch"
            
    def test_enum_domain_consistency(self):
        """Test that Python enums match CSV domain tables."""
        from src.baliza.enums import ModalidadeContratacao, SituacaoContratacao
        
        # Test modalidade consistency
        modalidade_csv = pd.read_csv("dbt_baliza/seeds/domain_tables/modalidade_contratacao.csv")
        enum_values = {m.value for m in ModalidadeContratacao}
        csv_values = set(modalidade_csv["code"])
        
        # Enums should be subset of CSV (CSV is authoritative)
        missing_in_csv = enum_values - csv_values
        assert len(missing_in_csv) == 0, f"Enum values not in CSV: {missing_in_csv}"
        
        # Test situacao consistency  
        situacao_csv = pd.read_csv("dbt_baliza/seeds/domain_tables/situacao_contratacao.csv")
        enum_values = {s.value for s in SituacaoContratacao}
        csv_values = set(situacao_csv["code"])
        
        missing_in_csv = enum_values - csv_values  
        assert len(missing_in_csv) == 0, f"Situacao enum values not in CSV: {missing_in_csv}"
        
    def test_data_quality_validations(self, con):
        """Test data quality rules on transformed data."""
        load_test_data_as_bronze(con)
        run_ibis_pipeline(con)
        
        # Gold layer quality checks
        gold = con.table("gold_contratacoes_analytics").execute()
        
        # Business rule validations
        assert all(gold["quantidade_contratacoes"] > 0), "Contract counts should be positive"
        assert all(gold["valor_total_estimado"] >= 0), "Values should be non-negative"
        
        # Data completeness
        assert gold["anoContratacao"].notna().all(), "Year should always be present"
        assert gold["modalidadeNome"].notna().any(), "Should have some modalidade names"
        
    @pytest.mark.slow
    def test_performance_benchmarks(self, con):
        """Test pipeline performance with larger datasets."""
        # Create larger test dataset by duplicating test data
        base_records = load_test_data_as_bronze(con, "contratacoes_publicacao")
        
        # Duplicate records to simulate larger dataset
        large_records = []
        for i in range(100):  # 1000 total records
            for record in base_records:
                new_record = record.copy()
                new_record["numeroControlePNCP"] = f"{record['numeroControlePNCP']}-{i}"
                large_records.append(new_record)
        
        con.create_table("bronze_pncp_raw", ibis.memtable(large_records), overwrite=True)
        
        # Run pipeline and measure performance
        metrics = run_ibis_pipeline(con)
        
        # Performance assertions
        assert metrics["total_time"] < 60, "Large dataset should process in under 1 minute"
        assert metrics["tables_created"] >= 5, "All tables should be created"
        
        # Verify data volume
        gold_count = con.table("gold_contratacoes_analytics").count().execute()
        assert gold_count > 0, "Should produce analytical results"
```

### 5. Performance Monitoring & Optimization 📊 Priority: MEDIUM

**Create**: `pipelines/monitoring.py`

```python
import time
import psutil
from dataclasses import dataclass
from typing import Dict, Any
from src.baliza.ui import get_console

@dataclass
class PipelineMetrics:
    """Pipeline execution metrics for monitoring."""
    total_time: float
    step_times: Dict[str, float]
    memory_usage_mb: float
    tables_created: int
    records_processed: int
    domain_tables_loaded: int

class PipelineMonitor:
    """Monitor pipeline execution for performance optimization."""
    
    def __init__(self):
        self.console = get_console()
        self.start_time = None
        self.step_times = {}
        self.memory_start = None
        
    def start_monitoring(self):
        """Start pipeline monitoring."""
        self.start_time = time.time()
        self.memory_start = psutil.Process().memory_info().rss / 1024 / 1024
        self.console.print("📊 [blue]Pipeline monitoring started[/blue]")
        
    def log_step(self, step_name: str, records_processed: int = 0):
        """Log completion of a pipeline step."""
        if step_name not in self.step_times:
            self.step_times[step_name] = {"start": time.time()}
        else:
            duration = time.time() - self.step_times[step_name]["start"]
            self.step_times[step_name]["duration"] = duration
            self.step_times[step_name]["records"] = records_processed
            
            rate = f" ({records_processed/duration:.0f} records/sec)" if records_processed > 0 else ""
            self.console.print(f"  ✅ {step_name}: {duration:.2f}s{rate}")
            
    def get_metrics(self, con, domain_tables_count: int = 0) -> PipelineMetrics:
        """Get final pipeline metrics."""
        total_time = time.time() - self.start_time
        current_memory = psutil.Process().memory_info().rss / 1024 / 1024
        memory_usage = current_memory - self.memory_start
        
        # Count total records processed
        total_records = 0
        try:
            tables = ["bronze_pncp_raw", "silver_contratacoes", "gold_contratacoes_analytics"]
            for table in tables:
                if table in con.list_tables():
                    count = con.table(table).count().execute()
                    total_records += count
        except Exception:
            pass
            
        return PipelineMetrics(
            total_time=total_time,
            step_times={k: v.get("duration", 0) for k, v in self.step_times.items()},
            memory_usage_mb=memory_usage,
            tables_created=len(con.list_tables()),
            records_processed=total_records,
            domain_tables_loaded=domain_tables_count
        )
```

## Migration Strategy

### Phase 1: Critical Fixes (Week 1)
- [ ] Fix missing import in `pipelines/ibis_pipeline.py`
- [ ] Add basic domain table loading functionality
- [ ] Update tests to use real test data
- [ ] Verify pipeline runs end-to-end

### Phase 2: Domain Integration (Week 2)
- [ ] Implement complete domain table enrichment
- [ ] Add enum validation against CSV tables
- [ ] Create comprehensive E2E test suite
- [ ] Add data quality validations

### Phase 3: Monitoring & Optimization (Week 3)
- [ ] Implement performance monitoring
- [ ] Add memory usage tracking
- [ ] Create benchmark tests
- [ ] Optimize for larger datasets

### Phase 4: Documentation & Deployment (Week 4)
- [ ] Update CLI help and documentation
- [ ] Add migration guide from dbt
- [ ] Performance tuning recommendations
- [ ] Production deployment guidelines

## Benefits of Enhanced Implementation

### 🎯 **Data Quality**
- Real test data ensures production-like validation
- Domain table enrichment provides business context
- Enum validation prevents data inconsistencies

### 🚀 **Performance**
- Ibis lazy evaluation for memory efficiency
- Monitoring identifies bottlenecks
- Optimized for large-scale processing

### 🛠️ **Maintainability**
- Clear separation of concerns
- Comprehensive test coverage
- Performance monitoring and alerting

### 📊 **Business Value**
- Human-readable analytics with domain enrichment
- Consistent business logic enforcement
- Reliable data transformation pipeline

## Next Actions

1. **Immediate**: Fix the import issue to make branch functional
2. **Short-term**: Implement real test data integration
3. **Medium-term**: Add domain table enrichment system
4. **Long-term**: Complete monitoring and optimization

This enhancement plan transforms the Ibis pipeline from a basic proof-of-concept into a production-ready, enterprise-grade data transformation system that leverages all existing BALIZA assets effectively.