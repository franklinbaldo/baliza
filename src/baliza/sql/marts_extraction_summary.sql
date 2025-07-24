-- Create extraction summary mart table
CREATE OR REPLACE TABLE marts.extraction_summary AS
SELECT
    ingestion_date,
    endpoint,
    COUNT(*) as request_count,
    SUM(payload_size) as total_bytes,
    ROUND(SUM(payload_size) / 1024.0 / 1024.0, 2) as total_mb,
    MIN(collected_at) as first_extraction,
    MAX(collected_at) as last_extraction,
    COUNT(DISTINCT payload_sha256) as unique_payloads
FROM raw.api_requests
WHERE http_status = 200
GROUP BY ingestion_date, endpoint
ORDER BY ingestion_date DESC, endpoint