-- Create data quality monitoring table
CREATE OR REPLACE TABLE marts.data_quality AS
SELECT
    DATE_TRUNC('day', collected_at) as date,
    endpoint,
    -- Basic quality metrics
    COUNT(*) as total_requests,
    COUNT(CASE WHEN http_status = 200 THEN 1 END) as successful_requests,
    COUNT(CASE WHEN http_status != 200 THEN 1 END) as failed_requests,
    ROUND(
        COUNT(CASE WHEN http_status = 200 THEN 1 END) * 100.0 / COUNT(*), 2
    ) as success_rate_pct,
    -- Size metrics
    AVG(payload_size) as avg_payload_size,
    MIN(payload_size) as min_payload_size, 
    MAX(payload_size) as max_payload_size,
    -- Duplication detection
    COUNT(DISTINCT payload_sha256) as unique_payloads,
    COUNT(*) - COUNT(DISTINCT payload_sha256) as duplicate_payloads
FROM raw.api_requests
GROUP BY DATE_TRUNC('day', collected_at), endpoint
ORDER BY date DESC, endpoint