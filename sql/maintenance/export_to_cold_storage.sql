-- File: sql/maintenance/export_to_cold_storage.sql
-- Purpose: Export old requests to cold storage and remove from hot tier
-- Author: BALIZA Database Refactor
-- Created: 2025-01-XX
-- Dependencies: psa.pncp_requests_hot, psa.pncp_requests_cold
-- Parameters: {year}, {month}

COPY (
    SELECT * FROM psa.pncp_requests_hot
    WHERE extracted_at < CURRENT_DATE - INTERVAL '90 days'
) TO 's3://baliza-archive/${year}-${month}/requests.parquet' (
    PARQUET_VERSION v2,
    COMPRESSION zstd,
    ROW_GROUP_SIZE 500000
);

INSERT INTO psa.pncp_requests_cold
SELECT * FROM psa.pncp_requests_hot
WHERE extracted_at < CURRENT_DATE - INTERVAL '90 days';

DELETE FROM psa.pncp_requests_hot
WHERE extracted_at < CURRENT_DATE - INTERVAL '90 days';

CHECKPOINT;
