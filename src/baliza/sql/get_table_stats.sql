-- Get table statistics for monitoring
SELECT 
    'raw.api_requests' as table_name,
    COUNT(*) as record_count,
    MIN(collected_at) as oldest_record,
    MAX(collected_at) as newest_record,
    COUNT(DISTINCT endpoint) as unique_endpoints,
    COUNT(DISTINCT ingestion_date) as unique_dates,
    AVG(payload_size) as avg_payload_size,
    SUM(payload_size) as total_payload_size
FROM raw.api_requests

UNION ALL

SELECT 
    'meta.execution_log' as table_name,
    COUNT(*) as record_count,
    MIN(start_time) as oldest_record,
    MAX(start_time) as newest_record,
    COUNT(DISTINCT flow_name) as unique_endpoints,
    COUNT(DISTINCT DATE(start_time)) as unique_dates,
    AVG(records_processed) as avg_payload_size,
    SUM(records_processed) as total_payload_size
FROM meta.execution_log

UNION ALL

SELECT 
    'meta.failed_requests' as table_name,
    COUNT(*) as record_count,
    MIN(failed_at) as oldest_record,
    MAX(failed_at) as newest_record,
    COUNT(DISTINCT original_endpoint) as unique_endpoints,
    COUNT(DISTINCT DATE(failed_at)) as unique_dates,
    AVG(retry_count) as avg_payload_size,
    SUM(CASE WHEN resolved THEN 1 ELSE 0 END) as total_payload_size
FROM meta.failed_requests;