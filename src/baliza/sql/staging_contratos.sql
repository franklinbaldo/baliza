-- Create staging view for contratos
CREATE OR REPLACE VIEW staging.contratos AS
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
WHERE endpoint = 'contratos'
AND http_status = 200