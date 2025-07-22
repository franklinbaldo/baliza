-- File: sql/maintenance/cleanup_old_data.sql
-- Purpose: Delete old request data and orphaned content
-- Author: BALIZA Database Refactor
-- Created: 2025-01-XX
-- Dependencies: psa.pncp_requests, psa.pncp_content tables
-- Parameters:
-- ${SCHEMA_NAME} - Target schema (default: psa)
-- ${CUTOFF_DATE} - Delete records older than this date

DELETE FROM ${SCHEMA_NAME}.pncp_requests
WHERE data_date < ${CUTOFF_DATE};

DELETE FROM ${SCHEMA_NAME}.pncp_content
WHERE reference_count = 0;
