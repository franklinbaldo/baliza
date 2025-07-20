-- Create PSA schema
CREATE SCHEMA IF NOT EXISTS psa;

-- Table 1: Content storage with deduplication
CREATE TABLE IF NOT EXISTS psa.pncp_content (
    id UUID PRIMARY KEY, -- UUIDv5 based on content hash
    response_content TEXT NOT NULL,
    content_sha256 VARCHAR(64) NOT NULL UNIQUE, -- For integrity verification
    content_size_bytes INTEGER,
    first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reference_count INTEGER DEFAULT 1 -- How many requests reference this content
) WITH (compression = "zstd");

-- Table 2: Request metadata with foreign key to content
CREATE TABLE IF NOT EXISTS psa.pncp_requests (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    endpoint_url VARCHAR NOT NULL,
    endpoint_name VARCHAR NOT NULL,
    request_parameters JSON,
    response_code INTEGER NOT NULL,
    response_headers JSON,
    data_date DATE,
    run_id VARCHAR,
    total_records INTEGER,
    total_pages INTEGER,
    current_page INTEGER,
    page_size INTEGER,
    content_id UUID REFERENCES psa.pncp_content(id) -- Foreign key to content
) WITH (compression = "zstd");

-- Legacy table for backwards compatibility during migration
CREATE TABLE IF NOT EXISTS psa.pncp_raw_responses (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    endpoint_url VARCHAR NOT NULL,
    endpoint_name VARCHAR NOT NULL,
    request_parameters JSON,
    response_code INTEGER NOT NULL,
    response_content TEXT,
    response_headers JSON,
    data_date DATE,
    run_id VARCHAR,
    total_records INTEGER,
    total_pages INTEGER,
    current_page INTEGER,
    page_size INTEGER
) WITH (compression = "zstd");

-- Create the new control table
DROP TABLE IF EXISTS psa.pncp_extraction_tasks;
CREATE TABLE psa.pncp_extraction_tasks (
    task_id VARCHAR PRIMARY KEY,
    endpoint_name VARCHAR NOT NULL,
    data_date DATE NOT NULL,
    modalidade INTEGER,
    status VARCHAR DEFAULT 'PENDING' NOT NULL,
    total_pages INTEGER,
    total_records INTEGER,
    missing_pages JSON,
    last_error TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    CONSTRAINT unique_task UNIQUE (endpoint_name, data_date, modalidade)
);

CREATE SCHEMA IF NOT EXISTS main_planning;

CREATE TABLE IF NOT EXISTS main_planning.task_plan_meta (
    plan_fingerprint VARCHAR PRIMARY KEY,
    date_range_start DATE,
    date_range_end DATE,
    environment VARCHAR,
    task_count INTEGER,
    generated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS main_planning.task_plan (
    task_id VARCHAR PRIMARY KEY,
    plan_fingerprint VARCHAR,
    endpoint_name VARCHAR,
    data_date DATE,
    modalidade VARCHAR,
    status VARCHAR
);

CREATE SCHEMA IF NOT EXISTS main_runtime;

CREATE TABLE IF NOT EXISTS main_runtime.task_claims (
    claim_id VARCHAR PRIMARY KEY,
    task_id VARCHAR,
    claimed_at TIMESTAMP,
    expires_at TIMESTAMP,
    worker_id VARCHAR,
    status VARCHAR
);

CREATE TABLE IF NOT EXISTS main_runtime.task_results (
    result_id VARCHAR PRIMARY KEY,
    task_id VARCHAR,
    request_id VARCHAR,
    page_number INTEGER,
    records_count INTEGER,
    completed_at TIMESTAMP,
    status VARCHAR
);
