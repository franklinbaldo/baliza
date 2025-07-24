-- Create staging view for contratacoes_publicacao
CREATE OR REPLACE VIEW staging.contratacoes AS
SELECT 
    request_id,
    ingestion_date,
    endpoint,
    http_status,
    payload_sha256,
    payload_size,
    collected_at,
    -- Extract JSON fields from compressed payload
    -- Note: In real implementation, we'd decompress and parse JSON
    payload_compressed as raw_payload
FROM raw.api_requests 
WHERE endpoint = 'contratacoes_publicacao'
AND http_status = 200