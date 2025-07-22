{{
  config(
    materialized='incremental',
    unique_key='analysis_id'
  )
}}

WITH content_metrics AS (
    SELECT
        c.id as content_id,
        c.reference_count,
        c.content_size_bytes,
        r.endpoint_name,
        r.data_date
    FROM {{ source('bronze', 'pncp_content') }} c
    JOIN {{ source('bronze', 'pncp_requests') }} r ON c.id = r.content_id
)
SELECT
    -- Analysis ID
    {{ dbt_utils.generate_surrogate_key(['endpoint_name', 'data_date']) }} as analysis_id,

    -- Dimensions
    endpoint_name,
    data_date,

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
    
    -- Analysis timestamp
    CURRENT_TIMESTAMP as analysis_timestamp

FROM content_metrics
GROUP BY endpoint_name, data_date