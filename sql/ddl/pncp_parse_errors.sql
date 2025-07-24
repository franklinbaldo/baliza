CREATE TABLE IF NOT EXISTS pncp_parse_errors (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    url VARCHAR(500) NOT NULL,
    endpoint_name VARCHAR(50) NOT NULL,
    error_message TEXT NOT NULL,
    response_raw TEXT, -- Apenas para debugging
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    retry_count INTEGER DEFAULT 0,
    resolved_at TIMESTAMP NULL
);
