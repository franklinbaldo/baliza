# Baliza ETL Pipeline Testing Guide

This guide demonstrates how to test the complete optimized ETL pipeline with all the new features.

## 🚀 Quick Start Testing

### 1. Initialize Database
```bash
baliza init
baliza doctor
```

### 2. Test Optimized Extraction (October 2021)
```bash
# First run - extracts all data
baliza extract --mes 2021-10

# Second run - should skip existing pages (demonstrates optimization)
baliza extract --mes 2021-10
```

Expected behavior:
- **First run**: Fetches all pages, stores ~3700+ records
- **Second run**: Skips all URLs, returns cached data instantly

### 3. Test ETL Pipeline
```bash
# Transform raw data to staging + marts
baliza transform

# Export to Parquet files
baliza export --output data/parquet --mes 2021-10
```

### 4. Test Complete Pipeline
```bash
# Run everything at once
baliza run --mes 2021-10
```

## 🧪 Optimization Testing

### URL-Based Deduplication
Test that the system avoids redundant API calls:

```bash
# Extract specific month
baliza extract --mes 2021-10

# Check logs - should show "Found existing successful request for URL, skipping HTTP call"
# Re-run same command, should be nearly instant
baliza extract --mes 2021-10
```

### Pagination Optimization
Test intelligent page fetching:

```bash
# Interrupt extraction mid-way (Ctrl+C after some pages)
baliza extract --mes 2021-10

# Resume - should only fetch missing pages
baliza extract --mes 2021-10
```

### Storage Deduplication
Test payload-level deduplication:

```bash
# Query to check deduplication efficiency
baliza query "
SELECT 
    COUNT(*) as total_requests,
    COUNT(DISTINCT payload_sha256) as unique_payloads,
    COUNT(*) - COUNT(DISTINCT payload_sha256) as duplicates_avoided,
    ROUND((COUNT(*) - COUNT(DISTINCT payload_sha256)) * 100.0 / COUNT(*), 2) as dedup_percentage
FROM raw.api_requests
"
```

## 📊 Data Pipeline Testing

### Staging Layer Validation
Test JSON payload normalization:

```bash
# Check staging tables were created with normalized data
baliza query "SELECT COUNT(*) FROM staging.contratacoes"
baliza query "SELECT COUNT(*) FROM staging.contratos" 
baliza query "SELECT COUNT(*) FROM staging.atas"

# Verify source metadata is preserved
baliza query "
SELECT 
    source_endpoint,
    source_ingestion_date,
    COUNT(*) as records
FROM staging.contratacoes 
GROUP BY source_endpoint, source_ingestion_date
ORDER BY source_ingestion_date DESC
LIMIT 10
"
```

### Marts Layer Validation
Test analytics tables:

```bash
# Check extraction summary
baliza query "SELECT * FROM marts.extraction_summary ORDER BY ingestion_date DESC LIMIT 5"

# Check data quality metrics
baliza query "SELECT * FROM marts.data_quality ORDER BY date DESC LIMIT 5"

# Check endpoint performance
baliza query "SELECT * FROM marts.endpoint_performance ORDER BY total_requests DESC"
```

## 📦 Export Testing

### Basic Export
```bash
# Export all data
baliza export

# Export only staging data
baliza export --no-marts

# Export only marts data  
baliza export --no-staging

# Export to custom directory
baliza export --output /tmp/baliza_export
```

### Month-Filtered Export
```bash
# Export specific month data
baliza export --mes 2021-10 --output data/october_2021

# Check generated files
ls -la data/october_2021/
```

### Validate Parquet Files
```python
# Test reading exported Parquet files
import polars as pl  # or pandas as pd

# Read staging data
staging_df = pl.read_parquet("data/parquet/staging/")
print(f"Staging records: {len(staging_df)}")

# Read marts data
marts_df = pl.read_parquet("data/parquet/marts/")
print(f"Marts records: {len(marts_df)}")
```

## 🔍 Performance Benchmarks

### Extraction Speed Test
```bash
# Time first extraction (cold)
time baliza extract --mes 2021-10

# Time second extraction (should be ~instant due to deduplication)
time baliza extract --mes 2021-10
```

Expected results:
- **First run**: ~8-15 minutes (depending on network)
- **Second run**: ~1-3 seconds (cached data)

### Storage Efficiency Test
```bash
# Check database size and deduplication
baliza query "
SELECT 
    'Raw Requests' as layer,
    COUNT(*) as records,
    ROUND(SUM(payload_size) / 1024.0 / 1024.0, 2) as size_mb
FROM raw.api_requests
UNION ALL
SELECT 
    'Unique Payloads' as layer,
    COUNT(*) as records,
    ROUND(SUM(LENGTH(payload_compressed)) / 1024.0 / 1024.0, 2) as size_mb
FROM raw.hot_payloads
"
```

### Export Performance Test
```bash
# Time Parquet export
time baliza export --mes 2021-10
```

## 🧩 Integration Testing

### Full Month Pipeline
Test complete workflow for a full month:

```bash
#!/bin/bash
# Full pipeline test script

echo "🚀 Testing complete pipeline for October 2021..."

# Step 1: Extract
echo "📥 Step 1: Extracting raw data..."
baliza extract --mes 2021-10

# Step 2: Transform
echo "🔄 Step 2: Transforming to staging and marts..."
baliza transform

# Step 3: Export
echo "📦 Step 3: Exporting to Parquet..."
baliza export --mes 2021-10 --output data/october_2021_complete

# Step 4: Validate
echo "✅ Step 4: Validating results..."
baliza query "SELECT COUNT(*) as total_raw_requests FROM raw.api_requests"
baliza query "SELECT COUNT(*) as total_staging_records FROM staging.contratacoes"
ls -la data/october_2021_complete/

echo "🎉 Pipeline test completed!"
```

## 🐛 Error Scenarios Testing

### Network Interruption Recovery
```bash
# Start extraction, kill process mid-way, restart
baliza extract --mes 2021-10
# Kill with Ctrl+C after some progress
baliza extract --mes 2021-10  # Should resume from where it left off
```

### Dependency Fallbacks
Test graceful degradation when Polars is not available:

```bash
# Temporarily rename polars to test pandas fallback
pip uninstall polars
baliza transform  # Should use pandas fallback
baliza export     # Should use pandas for export
```

## 📈 Monitoring and Observability

### Check Deduplication Metrics
```bash
baliza query "
SELECT 
    endpoint,
    COUNT(*) as total_requests,
    COUNT(DISTINCT payload_sha256) as unique_payloads,
    ROUND((COUNT(*) - COUNT(DISTINCT payload_sha256)) * 100.0 / COUNT(*), 2) as dedup_efficiency_pct
FROM raw.api_requests 
GROUP BY endpoint
ORDER BY dedup_efficiency_pct DESC
"
```

### Monitor Extraction Progress
```bash
baliza query "
SELECT 
    ingestion_date,
    endpoint,
    COUNT(*) as requests,
    MIN(collected_at) as first_request,
    MAX(collected_at) as last_request
FROM raw.api_requests 
WHERE ingestion_date >= '2021-10-01' 
GROUP BY ingestion_date, endpoint
ORDER BY ingestion_date DESC, endpoint
"
```

## 🎯 Success Criteria

A successful test should demonstrate:

1. **⚡ Performance**: 90%+ reduction in time for re-runs
2. **💾 Storage**: 70%+ storage efficiency through deduplication  
3. **🔄 Resumption**: Interrupted extractions resume correctly
4. **📊 Data Quality**: All staging tables populated with normalized JSON
5. **📈 Analytics**: Marts tables provide meaningful business metrics
6. **📦 Export**: Clean Parquet files with proper partitioning
7. **🛡️ Robustness**: Graceful handling of errors and missing dependencies

## 🔧 Troubleshooting

### Common Issues

**Missing Dependencies**:
```bash
pip install polars ibis-framework[duckdb] prefect typer rich
```

**Database Issues**:
```bash
baliza reset --force  # Reset database if corrupted
baliza init            # Reinitialize
```

**Permission Issues**:
```bash
chmod -R 755 data/     # Fix directory permissions
```

This comprehensive testing guide ensures all optimizations and features work correctly in real-world scenarios.