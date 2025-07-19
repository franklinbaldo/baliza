{{
  config(
    materialized='view'
  )
}}

-- Content deduplication analysis model (ADR-008)
-- Provides insights into storage optimization achieved through split table architecture

SELECT
    -- Content metrics
    COUNT(*) as unique_content_records,
    SUM(reference_count) as total_references,
    SUM(content_size_bytes) as actual_storage_bytes,
    SUM(content_size_bytes * reference_count) as theoretical_storage_bytes,
    
    -- Deduplication metrics
    COUNT(CASE WHEN reference_count > 1 THEN 1 END) as deduplicated_content_count,
    (COUNT(CASE WHEN reference_count > 1 THEN 1 END)::FLOAT / COUNT(*)) * 100 as deduplication_rate_percent,
    
    -- Storage savings
    (SUM(content_size_bytes * reference_count) - SUM(content_size_bytes)) as storage_savings_bytes,
    ((SUM(content_size_bytes * reference_count) - SUM(content_size_bytes))::FLOAT / SUM(content_size_bytes * reference_count)) * 100 as storage_savings_percent,
    
    -- Compression metrics
    (SUM(content_size_bytes)::FLOAT / SUM(content_size_bytes * reference_count)) as compression_ratio,
    
    -- Content size distribution
    MIN(content_size_bytes) as min_content_size,
    MAX(content_size_bytes) as max_content_size,
    AVG(content_size_bytes) as avg_content_size,
    MEDIAN(content_size_bytes) as median_content_size,
    
    -- Reference distribution
    MIN(reference_count) as min_references,
    MAX(reference_count) as max_references,
    AVG(reference_count) as avg_references,
    MEDIAN(reference_count) as median_references,
    
    -- Analysis timestamp
    CURRENT_TIMESTAMP as analysis_timestamp

FROM {{ source('pncp', 'pncp_content') }}