-- File: sql/analytics/endpoint_performance.sql
-- Purpose: Summarize request counts and success rates per endpoint
-- Author: BALIZA Database Refactor
-- Created: 2025-01-XX
-- Dependencies: psa.pncp_requests table
-- Parameters:
-- ${SCHEMA_NAME} - Target schema (default: psa)
-- ${DATE_FILTER} - Optional date filter

SELECT
    endpoint_name,
    COUNT(*) AS total_requests,
    COUNT(CASE WHEN response_code = 200 THEN 1 END) AS success_requests,
    ROUND(
        COUNT(CASE WHEN response_code = 200 THEN 1 END)::FLOAT / COUNT(*) * 100,
        1
    ) AS success_rate_pct
FROM ${SCHEMA_NAME}.pncp_requests
WHERE 1=1
${DATE_FILTER}
GROUP BY endpoint_name
ORDER BY endpoint_name;
