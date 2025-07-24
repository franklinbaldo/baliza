# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Baliza is a modern ETL pipeline for PNCP (Portal Nacional de Contratações Públicas) data using Prefect, Ibis, and DuckDB. The project implements a three-layer architecture: Raw → Staging → Marts with comprehensive data quality monitoring and audit capabilities.

## Key Commands

### Installation and Setup
```bash
# Install the package in development mode
pip install -e .

# Initialize the environment (creates data/ directory and DuckDB database)
baliza init

# Check system health and dependencies
baliza doctor
```

### Core ETL Operations
```bash
# Run complete ETL pipeline for latest month
baliza run --latest

# Run for specific month
baliza run --mes 2024-01

# Run for specific day (advanced)
baliza run --dia 2024-01-15

# Extract only (raw layer)
baliza extract --mes 2024-01

# Transform only (staging + marts)
baliza transform --mes 2024-01
```

### Data Management
```bash
# Execute SQL queries directly
baliza query "SELECT COUNT(*) FROM raw.audit_log"

# Export database schema to YAML
baliza dump-catalog

# Reset database (with confirmation)
baliza reset

# Force reset without confirmation
baliza reset --force
```

### Data Integrity and Monitoring
```bash
# Verify data integrity for date range
baliza verify --range 2024-01-01:2024-03-31

# Verify random sample (10%)
baliza verify --sample 0.1

# Verify all historical data (expensive operation)
baliza verify --all

# Fetch specific payload by SHA-256 hash
baliza fetch-payload <sha256_hash>
```

### Development Tools
```bash
# Start Prefect UI for monitoring flows
baliza ui

# The CLI entry point is defined in pyproject.toml as 'baliza = "baliza.cli:app"'
```

### Concurrent Extraction (No Rate Limiting)
```bash
# PNCP API has no explicit rate limiting - can use all endpoints simultaneously
baliza extract --concurrent --all-endpoints --date-range last_30_days

# Extract specific modalidades in parallel 
baliza extract --concurrent --modalidades 6,7,8 --max-workers 12

# Performance: 8.75x faster than sequential (8 min vs 70 min per month)
```

## Architecture Overview

### Data Layers
1. **Raw Layer**: Immutable audit log (`raw.audit_log`) with deduplicated payload storage (`raw.hot_payloads`)
2. **Staging Layer**: Ibis views for data transformation and cleaning
3. **Marts Layer**: Materialized Parquet tables partitioned by year/month

### Key Components
- `src/baliza/cli.py`: Typer-based CLI interface with all commands
- `src/baliza/backend.py`: Ibis/DuckDB connection management
- `src/baliza/config.py`: Pydantic settings for API URLs and database paths
- `src/baliza/flows/`: Prefect flow definitions for each ETL stage
- `src/baliza/metrics/`: Data quality monitoring and metrics collection
- `src/baliza/pncp_schemas.py`: Pydantic models for PNCP API responses
- `src/baliza/enums.py`: Enumeration definitions

### Database Schema
- `raw.audit_log`: Timestamped log of all API requests with SHA-256 payload hashes
- `raw.hot_payloads`: Deduplicated blob storage for JSON payloads
- `meta.metrics_log`: Pipeline execution metrics and data quality measures
- `meta.divergence_log`: Records of data changes detected during verification

### Data Storage Strategy
- Daily partitioning in raw layer for surgical reprocessing
- Content-addressable storage using SHA-256 hashes for deduplication
- S3/Glacier backup for divergent payloads during integrity checks

## Development Notes

### Configuration
- Database path: `data/baliza.duckdb` (configurable via `DATABASE_PATH`)
- PNCP API base: `https://pncp.gov.br/api/consulta`
- Settings loaded from `.env` file if present

### Flow Architecture
- Extraction flows write to raw layer only
- Transformation flows create Ibis views in staging
- Load flows materialize marts with partitioning/clustering
- Verification flows run integrity checks against historical data

### Error Handling
- API version validation before each run
- Schema drift detection with automatic alerts
- Comprehensive logging and metrics collection
- **No explicit rate limiting** - uses pagination and server capacity management
- Circuit breakers for 500 errors and server overload protection
- Adaptive concurrency (12 endpoints simultaneously, 120 req/min)

The project follows modern data engineering patterns with emphasis on data lineage, observability, and defensive programming practices.