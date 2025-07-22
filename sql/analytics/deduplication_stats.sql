-- File: sql/analytics/deduplication_stats.sql
-- Purpose: Calculate storage efficiency from content deduplication
-- Author: BALIZA Database Refactor
-- Created: 2025-01-XX
-- Dependencies: psa.pncp_content table
-- Performance: ~100ms on 1M records

-- Parameters (to be replaced by calling code):
-- ${SCHEMA_NAME} - Target schema (default: psa)
-- ${DATE_FILTER} - Optional date range filter

WITH dedup_analysis AS (
    SELECT
        COUNT(*) as unique_content,
        SUM(reference_count) as total_references,
        COUNT(CASE WHEN reference_count > 1 THEN 1 END) as deduplicated,
        SUM(content_size_bytes) as actual_size,
        SUM(content_size_bytes * reference_count) as theoretical_size
    FROM ${SCHEMA_NAME}.pncp_content
    WHERE 1=1
    ${DATE_FILTER}
)
SELECT
    unique_content,
    total_references,
    deduplicated,
    ROUND(deduplicated::FLOAT / unique_content * 100, 1) as dedup_rate_pct,
    actual_size,
    theoretical_size,
    (theoretical_size - actual_size) as bytes_saved,
    ROUND((theoretical_size - actual_size)::FLOAT / theoretical_size * 100, 1) as storage_efficiency_pct
FROM dedup_analysis;
