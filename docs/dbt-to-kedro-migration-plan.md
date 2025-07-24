# dbt to Kedro Migration Plan + PSA Removal

**Strategic Data Pipeline Modernization**  
**Date**: July 24, 2025  
**Status**: ⚠️ DEFERRED - Focusing on Ibis Pipeline First

## ⚠️ Current Status Update

**Migration Status**: The Kedro migration has been **temporarily deferred** due to CLI compatibility issues. Current focus is on:

1. **✅ PRIMARY**: Ibis pipeline (ADR-014) - **Currently working and stable**
2. **❌ REMOVED**: dbt completely abandoned - too complex, replaced by Ibis
3. **🚧 FUTURE**: Kedro + Ibis integration when CLI refactoring is complete

**Architecture Decision**: **dbt → Ibis + Kedro migration** eliminates dbt complexity while gaining production-ready orchestration.
**Performance Strategy**: **Ibis-native** processing (no pandas needed), optional **Polars** for specific use cases.
**Technical Blocker**: Kedro (Click) vs BALIZA (Typer) CLI incompatibility requires resolution first.

## Executive Summary

This plan outlines the complete replacement of dbt with Kedro as our data pipeline orchestration framework, while simultaneously removing the PSA (Persistent Staging Area) to adopt a simpler, more modern data architecture. This change aligns with current industry trends towards simpler, more maintainable data pipelines.

## Current State Analysis

### Current Architecture
```
PNCP API → Extractor → PSA (psa.raw_*) → Ibis/dbt Pipeline → Raw → Stage → Mart
```

### Problems with Current Setup
1. **Dual Pipeline Complexity**: Both Ibis and dbt create parallel transformation logic
2. **PSA Overhead**: Data Vault 2.0 PSA pattern adds unnecessary complexity for our use case
3. **Tool Fragmentation**: Multiple tools (dbt, Ibis, custom Python) for data transformation
4. **Storage Duplication**: PSA → Raw layer creates data duplication without clear benefit

## Target Architecture

### New Simplified Flow
```
PNCP API → Kedro Pipeline → Raw → Stage → Mart (All in DuckDB)
```

### Key Benefits
- **Single Pipeline Framework**: Kedro handles all transformation logic
- **Eliminated PSA**: Direct Raw → Stage → Mart flow (true Medallion)
- **Python-First**: All transformations in Python with strong typing
- **Better Testing**: Kedro's built-in testing framework for data pipelines
- **Simplified Storage**: Remove duplicate storage layers

## Why Remove PSA?

### PSA Use Cases We Don't Need
| PSA Benefit | Our Reality |
|-------------|-------------|
| **Full Change History** | PNCP API provides historical data; we don't need source-level audit |
| **Easy Restatements** | Raw layer reprocessing is sufficient for our use case |
| **Regulatory Compliance** | Brazilian procurement data doesn't require Data Vault level audit trails |
| **Source System Protection** | PNCP is stable; we don't need insert-only protection |

### Our Use Case Alignment
✅ **Research & Analytics Focus**: Need clean, analytics-ready data  
✅ **Public Data Source**: PNCP is reliable, no need for source protection  
✅ **Cost Optimization**: Storage efficiency matters more than audit trails  
✅ **Simplicity**: Easier pipeline = better maintainability  

## Phase 1: Kedro Pipeline Foundation (Week 1-2)

### 1.1 Kedro Project Setup
```bash
# Install Kedro
uv add kedro kedro-datasets[pandas,duckdb]

# Initialize Kedro project structure within baliza
kedro new --config=pyproject.toml
```

### 1.2 Core Pipeline Structure
```
src/baliza/pipelines/
├── data_extraction/          # Extract from PNCP API
│   ├── nodes.py              # Extraction functions
│   ├── pipeline.py           # Extraction pipeline
│   └── __init__.py
├── data_processing/          # Raw → Stage transformations  
│   ├── nodes.py              # Cleaning, validation
│   ├── pipeline.py           # Processing pipeline
│   └── __init__.py
├── analytics/                # Stage → Mart transformations
│   ├── nodes.py              # Aggregations, analytics
│   ├── pipeline.py           # Analytics pipeline
│   └── __init__.py
└── domain_enrichment/        # Domain table integration
    ├── nodes.py              # Domain lookups
    ├── pipeline.py           # Enrichment pipeline
    └── __init__.py
```

### 1.3 Data Catalog Configuration
```yaml
# conf/base/catalog.yml
raw_pncp_data:
  type: duckdb.DuckDBDataSet
  connection: ${duckdb_connection}
  table_name: raw_pncp_data
  
stage_contratacoes:
  type: duckdb.DuckDBDataSet  
  connection: ${duckdb_connection}
  table_name: stage_contratacoes

mart_analytics:
  type: duckdb.DuckDBDataSet
  connection: ${duckdb_connection}
  table_name: mart_analytics
```

## Phase 2: PSA Elimination (Week 2-3)

### 2.1 Direct API to Raw Flow
Remove PSA tables and implement direct write to Raw layer:

**Before:**
```
PNCP API → psa.raw_pncp_requests → psa.raw_contratos → Raw Layer
```

**After:**
```
PNCP API → Raw Layer (direct write)
```

### 2.2 Schema Simplification
```sql
-- Remove PSA schema entirely
DROP SCHEMA IF EXISTS psa CASCADE;

-- Simplified raw tables (no intermediate PSA)
CREATE SCHEMA raw;
CREATE TABLE raw.contratos (...);  -- Direct from API
CREATE TABLE raw.contratacoes (...);
CREATE TABLE raw.atas (...);
```

### 2.3 Modified Extractor Logic
```python
class DirectRawExtractor:
    """Writes directly to Raw layer, bypassing PSA"""
    
    async def extract_and_store(self, endpoint_data):
        # Parse and validate API response
        validated_data = self.validate_pncp_data(endpoint_data)
        
        # Write directly to raw layer
        await self.write_to_raw_table(validated_data)
        
        # No PSA intermediate step
```

## Phase 3: Kedro Pipeline Migration (Week 3-4)

### 3.1 Extract Pipeline (Replace current extractor)
```python
# src/baliza/pipelines/data_extraction/nodes.py
def extract_pncp_data(date_range: Dict) -> pd.DataFrame:
    """Extract data from PNCP API for given date range"""
    extractor = AsyncPNCPExtractor()
    return extractor.extract_for_range(date_range)

def validate_raw_data(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Validate and clean raw PNCP data"""
    return validate_pncp_schemas(raw_data)
```

### 3.2 Processing Pipeline (Replace dbt staging)
```python
# src/baliza/pipelines/data_processing/nodes.py
def clean_contratacoes(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Transform raw to stage layer"""
    return apply_contratacoes_transformations(raw_data)

def create_dimensional_tables(raw_data: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Create dimensional tables for stage layer"""
    return {
        'dim_organizacoes': extract_org_dimension(raw_data),
        'dim_unidades': extract_unit_dimension(raw_data)
    }
```

### 3.3 Analytics Pipeline (Replace dbt marts)
```python
# src/baliza/pipelines/analytics/nodes.py
def create_procurement_analytics(stage_data: pd.DataFrame) -> pd.DataFrame:
    """Create analytical mart for procurement insights"""
    return aggregate_procurement_metrics(stage_data)

def create_spending_summary(stage_data: pd.DataFrame) -> pd.DataFrame:
    """Create spending analysis mart"""
    return analyze_spending_patterns(stage_data)
```

## Phase 4: dbt Removal (Week 4-5)

### 4.1 Remove dbt Infrastructure
```bash
# Remove entire dbt project
rm -rf dbt_baliza/

# Remove dbt dependencies
uv remove dbt-core dbt-duckdb

# Update pyproject.toml - remove dbt sections
```

### 4.2 Update CLI Integration
```python
# Replace in src/baliza/cli.py
@app.command()
def transform():
    """Transform raw data using Kedro pipelines"""
    from kedro.framework.session import KedroSession
    
    with KedroSession.create() as session:
        session.run(pipeline_name="data_processing")
        session.run(pipeline_name="analytics")
```

### 4.3 Configuration Migration
Move all configuration from dbt to Kedro:
- Domain tables → Kedro datasets
- Pipeline parameters → Kedro parameters
- Environment configs → Kedro environments

## Phase 5: Testing & Validation (Week 5-6)

### 5.1 Kedro Pipeline Tests
```python
# tests/pipelines/test_data_processing.py
def test_contratacoes_pipeline():
    """Test complete contratacoes processing pipeline"""
    with KedroSession.create() as session:
        result = session.run(
            pipeline_name="data_processing",
            inputs={"raw_contratacoes": test_data}
        )
        assert result["stage_contratacoes"] is not None
```

### 5.2 E2E Pipeline Validation
```python
def test_full_kedro_pipeline():
    """Test complete Raw → Stage → Mart flow"""
    pipeline = Pipeline([
        extraction_pipeline,
        processing_pipeline, 
        analytics_pipeline
    ])
    
    result = pipeline.run()
    validate_mart_data_quality(result)
```

### 5.3 Performance Benchmarking
Compare new architecture against current:
- **Pipeline Execution Time**
- **Storage Efficiency** (without PSA duplication)
- **Memory Usage**
- **Data Quality Metrics**

## File Structure After Migration

```
baliza/
├── src/baliza/
│   ├── pipelines/
│   │   ├── data_extraction/      # PNCP API extraction
│   │   ├── data_processing/      # Raw → Stage
│   │   ├── analytics/            # Stage → Mart
│   │   └── domain_enrichment/    # Domain table integration
│   ├── cli.py                    # Updated CLI with Kedro integration
│   └── ...
├── conf/
│   ├── base/
│   │   ├── catalog.yml           # Kedro data catalog
│   │   ├── parameters.yml        # Pipeline parameters
│   │   └── pipelines.yml         # Pipeline configuration
│   └── local/                    # Local environment configs
├── data/                         # DuckDB storage (no PSA)
│   └── baliza.duckdb            # Single database file
├── tests/
│   └── pipelines/               # Kedro pipeline tests
└── docs/
    └── kedro/                   # Kedro documentation
```

## Migration Benefits

### 🎯 Simplified Architecture
- **Single Framework**: Kedro handles all pipeline logic
- **No PSA Overhead**: Direct Raw → Stage → Mart flow
- **Unified Testing**: Kedro's testing framework for all pipelines

### 💰 Cost & Performance
- **50% Storage Reduction**: Eliminate PSA duplication
- **Faster Pipelines**: No intermediate PSA writes
- **Better Resource Usage**: Single framework optimization

### 🔧 Developer Experience  
- **Python-First**: All transformations in familiar Python
- **Type Safety**: Strong typing throughout pipelines
- **Better Debugging**: Kedro's pipeline visualization and debugging tools

### 📊 Operational Benefits
- **Simpler Deployment**: Single framework configuration
- **Better Monitoring**: Kedro's built-in pipeline monitoring
- **Easier Scaling**: Kedro's modular pipeline architecture

## Risk Mitigation

### Medium Risk Areas
- **Data Loss During Migration**: Backup current data before PSA removal
- **Performance Regression**: Benchmark new architecture thoroughly
- **Integration Issues**: Test CLI and MCP server integration carefully

### Mitigation Strategies
1. **Parallel Development**: Build Kedro alongside existing system
2. **Staged Rollout**: Migrate one pipeline at a time
3. **Feature Flags**: Allow switching between dbt and Kedro during transition
4. **Comprehensive Testing**: E2E tests for all migration phases

## Timeline Summary

| Week | Phase | Key Deliverables |
|------|-------|------------------|
| 1-2 | Kedro Foundation | Project setup, pipeline structure, data catalog |
| 2-3 | PSA Elimination | Remove PSA schema, direct API-to-Raw flow |
| 3-4 | Pipeline Migration | Convert dbt models to Kedro nodes |
| 4-5 | dbt Removal | Remove dbt project, update CLI integration |
| 5-6 | Testing & Validation | E2E tests, performance benchmarking |

**Total Duration**: 6 weeks  
**Effort**: 2-3 hours/day average

## Success Metrics

- ✅ **Pipeline Execution Time**: ≤ current dbt performance
- ✅ **Storage Efficiency**: 50%+ reduction from PSA removal  
- ✅ **Test Coverage**: 90%+ E2E test coverage for all pipelines
- ✅ **Data Quality**: 100% feature parity with current output
- ✅ **Developer Experience**: Simpler development workflow

---

**Next Steps**: 
1. Approve migration plan
2. Set up Kedro development environment
3. Begin Phase 1 implementation
4. Create detailed timeline with milestones

**Expected Outcome**: Modern, simplified data pipeline architecture aligned with industry best practices, eliminating unnecessary complexity while maintaining full functionality.