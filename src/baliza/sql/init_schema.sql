-- Initialize database schema for Baliza
-- Raw Layer: Unified API requests table (from implementation plan v3.0)

-- TODO: Add more constraints to the tables to improve data integrity.
-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS meta;

-- Raw Layer: API requests table (unified design)
CREATE TABLE IF NOT EXISTS raw.api_requests (
    request_id         UUID PRIMARY KEY,
    ingestion_date     DATE NOT NULL,
    endpoint           TEXT NOT NULL,
    http_status        SMALLINT NOT NULL,
    etag               TEXT,
    payload_sha256     VARCHAR(64) NOT NULL,
    payload_size       INT NOT NULL,
    collected_at       TIMESTAMPTZ NOT NULL,

    -- Constraints
    UNIQUE(endpoint, collected_at),
    CHECK(http_status >= 100 AND http_status < 600),
    CHECK(payload_size > 0),
    FOREIGN KEY (payload_sha256) REFERENCES raw.hot_payloads(payload_sha256)
);

CREATE TABLE IF NOT EXISTS raw.hot_payloads (
    payload_sha256     VARCHAR(64) PRIMARY KEY,
    payload_compressed BLOB NOT NULL,
    first_seen_at      TIMESTAMPTZ NOT NULL
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_api_requests_ingestion_date
ON raw.api_requests(ingestion_date);

CREATE INDEX IF NOT EXISTS idx_api_requests_payload_sha256
ON raw.api_requests(payload_sha256);

CREATE INDEX IF NOT EXISTS idx_api_requests_endpoint
ON raw.api_requests(endpoint);

CREATE INDEX IF NOT EXISTS idx_api_requests_collected_at
ON raw.api_requests(collected_at);

-- Metadata: Execution log
CREATE TABLE IF NOT EXISTS meta.execution_log (
    execution_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    flow_name         TEXT NOT NULL,
    task_name         TEXT,
    start_time        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    end_time          TIMESTAMPTZ,
    status            TEXT NOT NULL CHECK(status IN ('running', 'success', 'failed', 'partial_success')),
    records_processed INT NOT NULL DEFAULT 0,
    bytes_processed   BIGINT NOT NULL DEFAULT 0,
    error_message     TEXT,
    metadata          JSON
);

-- Metadata: Schema evolution tracking
CREATE TABLE IF NOT EXISTS meta.schema_versions (
    version_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    api_version        TEXT NOT NULL,
    schema_fingerprint VARCHAR(64) NOT NULL,
    detected_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_supported       BOOLEAN NOT NULL DEFAULT TRUE,
    migration_notes    TEXT
);

-- Metadata: Failed requests for recovery
CREATE TABLE IF NOT EXISTS meta.failed_requests (
    failure_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    original_endpoint   TEXT NOT NULL,
    failure_reason      TEXT NOT NULL,
    failed_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    retry_count         INT NOT NULL DEFAULT 0,
    last_retry_at       TIMESTAMPTZ,
    resolved            BOOLEAN NOT NULL DEFAULT FALSE,
    archived_payload    BLOB,
    request_metadata    JSON
);

-- Create initial schema version record
INSERT INTO meta.schema_versions (api_version, schema_fingerprint, migration_notes)
SELECT '1.0', 'v3.0-unified-schema', 'Initial schema creation with unified raw.api_requests table'
WHERE NOT EXISTS (
    SELECT 1 FROM meta.schema_versions WHERE api_version = '1.0'
);
