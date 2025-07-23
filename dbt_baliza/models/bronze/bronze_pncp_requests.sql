{{
  config(
    materialized='table',
    options={
      'default_compression': 'zstd'
    }
  )
}}

SELECT
  "url_path",
  "response_time",
  "extracted_at"
FROM {{ source('bronze', 'pncp_requests') }}
