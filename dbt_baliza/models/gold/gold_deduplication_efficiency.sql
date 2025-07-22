{{
  config(
    materialized='view'
  )
}}

WITH raw_counts AS (
  SELECT COUNT(*) as raw_row_count FROM {{ ref('bronze_pncp_raw') }}
),
deduplicated_counts AS (
  SELECT COUNT(*) as deduplicated_row_count FROM {{ ref('silver_contratacoes') }}
)
SELECT
  raw_row_count,
  deduplicated_row_count,
  (1 - (deduplicated_row_count::FLOAT / raw_row_count::FLOAT)) * 100 AS deduplication_percentage
FROM raw_counts, deduplicated_counts
