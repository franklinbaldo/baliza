-- Create staging view for atas
CREATE OR REPLACE VIEW staging.atas AS
SELECT 
    request_id,
    ingestion_date,
    endpoint,
    http_status,
    payload_sha256,
    payload_size,
    collected_at,
    payload_compressed as raw_payload
FROM raw.api_requests 
WHERE endpoint = 'atas'
AND http_status = 200