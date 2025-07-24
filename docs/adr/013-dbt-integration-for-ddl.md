# ADR-013: dbt Integration for DDL Management

## Status
❌ **SUPERSEDED** by ADR-014 (Ibis Pipeline Adoption)

## ⚠️ Deprecation Notice
**Date**: July 2025  
**Reason**: dbt was **completely abandoned** due to excessive complexity  
**Replacement**: Ibis + Kedro pipeline architecture  
**Impact**: All dbt infrastructure removed from project

## Context
Database schema management in BALIZA currently lacks:
- Version control for schema changes
- Declarative data transformations
- Proper medallion architecture implementation
- Standardized data quality testing
- Automated dependency management between tables/views

dbt (Data Build Tool) provides a modern approach to data transformation and schema management that aligns with BALIZA's medallion architecture goals.

## Decision
We will integrate dbt as the primary tool for DDL management and data transformations.

**dbt Integration Strategy:**
1. **Schema as Code** - All DDL defined in dbt models, not raw SQL
2. **Medallion Architecture** - Bronze/Silver/Gold layers as dbt materializations
3. **Data Quality Testing** - Built-in dbt tests for validation
4. **Incremental Processing** - dbt incremental models for efficient updates
5. **Macro Library** - Reusable dbt macros for common patterns

**Project Structure:**
```
dbt_baliza/
├── models/
│   ├── bronze/     # Raw PNCP data with minimal processing
│   ├── silver/     # Cleaned, validated, type-enforced data  
│   ├── gold/       # Business logic, aggregations, analytics
│   └── marts/      # Final data products for consumption
├── macros/
│   ├── compression/    # Compression strategy macros
│   ├── validation/     # Data quality validation
│   └── optimization/   # Performance optimization
├── tests/          # Data quality tests
└── snapshots/      # SCD (Slowly Changing Dimensions)
```

**Materialization Strategy:**
- Bronze: `materialized: table` with raw data retention
- Silver: `materialized: table` with data quality enforcement  
- Gold: `materialized: table` or `view` based on usage patterns
- Hot/Cold tiers: Separate materializations based on data age

## Consequences

### Positive
- Declarative schema management with version control
- Built-in testing framework for data quality
- Incremental processing reduces processing time
- Clear separation of concerns (Bronze/Silver/Gold)
- Automatic dependency resolution
- Documentation generation
- Integration with modern data stack

### Negative
- Additional learning curve for SQL-focused team
- dbt-specific syntax and concepts
- Potential over-engineering for simple transformations
- Another tool to maintain and monitor

## Implementation Notes
- Use `dbt-duckdb` adapter for optimal DuckDB integration
- Implement compression macros following ADR-010 principles
- Create enum drift detection macros per ADR-011
- Align with hot-cold tiering strategy from ADR-012
- Generate DDL through `dbt run` instead of manual SQL

## References
- Database Optimization Plan Section 3: "Nova DDL Robusta com Integração dbt"  
- ADR-003: Medallion Architecture
- ADR-010: Compression Heuristics First
- ADR-011: Official PNCP Schema Compliance