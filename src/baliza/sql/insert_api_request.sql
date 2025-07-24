-- Insert API request data into raw.api_requests
-- Parameters: request_id, ingestion_date, endpoint, http_status, etag, payload_sha256, payload_size, payload_compressed, collected_at

INSERT INTO raw.api_requests (
    request_id,
    ingestion_date,
    endpoint,
    http_status,
    etag,
    payload_sha256,
    payload_size,
    payload_compressed,
    collected_at
) VALUES (
    ?,  -- request_id
    ?,  -- ingestion_date
    ?,  -- endpoint
    ?,  -- http_status
    ?,  -- etag
    ?,  -- payload_sha256
    ?,  -- payload_size
    ?,  -- payload_compressed
    ?   -- collected_at
);