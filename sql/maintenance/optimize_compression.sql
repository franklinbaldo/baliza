-- File: sql/maintenance/optimize_compression.sql
-- Purpose: Force compression and reclaim space
-- Author: BALIZA Database Refactor
-- Created: 2025-01-XX
-- Dependencies: DuckDB database
-- Parameters: None

CHECKPOINT;
VACUUM;
