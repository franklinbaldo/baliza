{{
  config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge'
  )
}}

WITH source_data AS (
    SELECT
        r.id,
        r.extracted_at,
        r.endpoint_name,
        r.endpoint_url,
        r.data_date,
        r.run_id,
        r.total_records,
        r.total_pages,
        r.current_page,
        c.response_content,
        c.content_sha256,
        c.content_size_bytes,
        c.reference_count
    FROM {{ source('pncp', 'pncp_requests') }} r
    JOIN {{ source('pncp', 'pncp_content') }} c ON r.content_id = c.id
    WHERE r.response_code = 200
      AND c.response_content IS NOT NULL
      AND c.response_content != ''
    {% if is_incremental() %}
      AND r.extracted_at > (SELECT MAX(extracted_at) FROM {{ this }})
    {% endif %}
),
endpoint_mapping AS (
    SELECT * FROM {{ ref('endpoints_config') }}
)
SELECT
    s.id,
    s.extracted_at,
    s.endpoint_name,
    s.endpoint_url,
    s.data_date,
    s.run_id,
    s.total_records,
    s.total_pages,
    s.current_page,
    TRY_CAST(s.response_content AS JSON) AS response_json,
    e.category AS endpoint_category,
    s.content_sha256,
    s.content_size_bytes,
    s.reference_count
FROM source_data s
LEFT JOIN endpoint_mapping e ON s.endpoint_name = e.endpoint_name
WHERE TRY_CAST(s.response_content AS JSON) IS NOT NULL