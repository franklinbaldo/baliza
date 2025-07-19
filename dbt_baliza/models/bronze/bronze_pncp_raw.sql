{{
  config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert'
  )
}}

-- Split table architecture (ADR-008): JOIN requests + content for complete data
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
    TRY_CAST(c.response_content AS JSON) AS response_json,
    -- Add endpoint category for easier downstream filtering
    CASE 
        WHEN r.endpoint_name IN ('atas_periodo', 'atas_atualizacao') THEN 'atas'
        WHEN r.endpoint_name IN ('contratos_publicacao', 'contratos_atualizacao') THEN 'contratos'
        WHEN r.endpoint_name IN ('contratacoes_publicacao', 'contratacoes_atualizacao', 'contratacoes_proposta') THEN 'contratacoes'
        ELSE 'other'
    END AS endpoint_category,
    -- Additional metadata from split tables
    c.content_sha256,
    c.content_size_bytes,
    c.reference_count  -- Useful for identifying frequently seen responses
FROM {{ source('pncp', 'pncp_requests') }} r
JOIN {{ source('pncp', 'pncp_content') }} c ON r.content_id = c.id
WHERE r.response_code = 200
  AND c.response_content IS NOT NULL
  AND c.response_content != ''
  AND TRY_CAST(c.response_content AS JSON) IS NOT NULL
{% if is_incremental() %}
  AND r.extracted_at > (SELECT MAX(extracted_at) FROM {{ this }})
{% endif %}