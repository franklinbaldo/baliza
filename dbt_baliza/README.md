# BALIZA dbt Project - Enhanced Data Architecture

## Overview
This dbt project implements the Medallion Architecture (Bronze/Silver/Gold) with enhanced data quality monitoring while preserving BALIZA's core principle of data transparency.

## Architecture Layers

### 📋 **Bronze Layer** (`models/bronze/`)
- **Purpose**: Raw data from PNCP API responses
- **Materialization**: Incremental (for performance)
- **Key Features**:
  - Minimal processing
  - JSON parsing and basic type conversion
  - Endpoint categorization
  - Incremental loading based on `extracted_at`

### 🧹 **Staging Layer** (`models/staging/`) - **NEW**
- **Purpose**: Standardized column names and data types
- **Materialization**: Views (for consistency)
- **Key Features**:
  - Snake_case column naming
  - Proper data type casting (`DECIMAL(15,2)` for monetary values)
  - Date standardization
  - Clean separation of concerns

### 🥈 **Silver Layer** (`models/silver/`)
- **Purpose**: Clean, deduplicated business entities
- **Materialization**: Incremental (for performance)
- **Key Features**:
  - Deduplication logic
  - Business rules application
  - Surrogate key generation
  - **Enhanced**: Now uses staging layer for cleaner logic

### 🥇 **Gold Layer** (`models/gold/`)
- **Purpose**: Business-ready analytical data marts
- **Materialization**: Tables/Views (depending on use case)
- **Key Features**:
  - Aggregated metrics
  - Business KPIs
  - Optimized for reporting and analysis

## Data Quality Philosophy

### 🔍 **Transparency over "Clean" Data**
BALIZA follows a unique data preservation philosophy:

- **❌ NEVER filter out "anomalous" data** (e.g., valor_inicial = 0)
- **✅ ALWAYS preserve original data** for transparency
- **📊 MONITOR anomalies** for citizen oversight
- **🔍 PROVIDE transparency reports** for data quality insights

### 🧪 **Enhanced Testing Strategy**
- **Relationship tests** between fact and dimension tables
- **Accepted values tests** for categorical data
- **Data preservation tests** to ensure zero values are maintained
- **Transparency monitoring** through custom macros

## Key Improvements

### 🚀 **Performance Enhancements**
1. **Incremental Models**: `silver_contratos` now uses incremental materialization
2. **Staging Layer**: Reduces complex transformations in silver layer
3. **Optimized Deduplication**: More efficient deduplication logic

### 🔧 **Code Quality**
1. **Separation of Concerns**: Clear separation between data cleansing and business logic
2. **Reusable Components**: Staging models can be reused across silver models
3. **Better Documentation**: Comprehensive column descriptions and testing

### 📊 **Data Quality Monitoring**
1. **Custom Macros**: `data_quality_monitoring.sql` for transparency reports
2. **Relationship Validation**: Ensures referential integrity between dimensions and facts
3. **Anomaly Tracking**: Monitors unusual patterns without filtering data

## Usage

### Running the Models
```bash
# Build all models
dbt build --profiles-dir dbt_baliza

# Build specific layers
dbt build --select models/staging --profiles-dir dbt_baliza
dbt build --select models/silver --profiles-dir dbt_baliza
dbt build --select models/gold --profiles-dir dbt_baliza

# Run tests
dbt test --profiles-dir dbt_baliza

# Generate documentation
dbt docs generate --profiles-dir dbt_baliza
dbt docs serve --profiles-dir dbt_baliza
```

### Data Quality Monitoring
```sql
-- View transparency report
SELECT * FROM silver_contratos_transparency_report;

-- Monitor specific anomalies
SELECT * FROM silver_contratos_quality_monitor;
```

## File Structure
```
dbt_baliza/
├── models/
│   ├── bronze/
│   │   ├── bronze_pncp_raw.sql
│   │   └── bronze_pncp_source.yml
│   ├── staging/          # NEW - Standardization layer
│   │   ├── stg_contratos.sql
│   │   ├── stg_atas.sql
│   │   ├── stg_contratacoes.sql
│   │   └── staging_schema.yml
│   ├── silver/           # ENHANCED - Uses staging layer
│   │   ├── silver_contratos.sql (now incremental)
│   │   ├── silver_*.sql
│   │   └── silver_schema.yml
│   └── gold/             # ENHANCED - Better testing
│       ├── mart_*.sql
│       └── gold_schema.yml
├── macros/
│   ├── extract_organization_data.sql
│   └── data_quality_monitoring.sql  # NEW - Transparency macros
├── tests/
│   └── data_preservation_test.sql    # NEW - Philosophy validation
└── README.md             # This file
```

## Benefits

1. **🏃 Performance**: Incremental models reduce processing time
2. **🔍 Transparency**: Enhanced monitoring without data filtering
3. **🧪 Quality**: Comprehensive testing with relationship validation
4. **📈 Scalability**: Clean architecture supports future growth
5. **🔧 Maintainability**: Staging layer simplifies silver model logic

## Migration from Previous Version

The enhanced architecture is backward compatible:
- Existing silver and gold models continue to work
- New staging layer provides additional benefits
- Enhanced testing catches more data quality issues
- Transparency monitoring adds new insights

## Key Principles

1. **Data Preservation**: Never filter data due to "anomalies"
2. **Transparency**: Provide clear visibility into data quality
3. **Performance**: Use incremental models for large datasets
4. **Testing**: Validate relationships and business rules
5. **Documentation**: Comprehensive metadata for all models

---

**Updated**: Following expert recommendations from TODO.md analysis
**Philosophy**: Transparency over "clean" data - citizens need raw truth to demand changes