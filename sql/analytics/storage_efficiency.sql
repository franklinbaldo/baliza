-- File: sql/analytics/storage_efficiency.sql
-- Purpose: Inspect storage usage and compression stats for a table
-- Author: BALIZA Database Refactor
-- Created: 2025-01-XX
-- Dependencies: DuckDB pragma_storage_info
-- Parameters:
-- ${TABLE_NAME} - Table name to analyze

SELECT
    table_name,
    SUM(total_blocks * block_size) AS bytes,
    SUM(CASE WHEN free_percent = 100 THEN 0 ELSE total_blocks END) AS used_blocks,
    ROUND(
        SUM(total_blocks * block_size * (1 - free_percent / 100.0))
    ) AS estimated_used_bytes
FROM pragma_storage_info('${TABLE_NAME}')
GROUP BY table_name;
