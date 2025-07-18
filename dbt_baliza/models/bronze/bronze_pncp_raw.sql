{{
  config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert'
  )
}}

WITH request_log AS (
    SELECT * FROM {{ source('pncp_parquet', 'request_log') }}
),
response_content AS (
    SELECT *
    FROM {{ source('pncp_parquet', 'response_content') }}
)

SELECT
    log.id,
    log.extracted_at,
    log.endpoint_name,
    log.data_date,
    log.run_id,
    log.current_page,
    TRY_CAST(content.response_content AS JSON) AS response_json,
    CASE 
        WHEN log.endpoint_name IN ('atas_periodo', 'atas_atualizacao') THEN 'atas'
        WHEN log.endpoint_name IN ('contratos_publicacao', 'contratos_atualizacao') THEN 'contratos'
        WHEN log.endpoint_name IN ('contratacoes_publicacao', 'contratacoes_atualizacao', 'contratacoes_proposta') THEN 'contratacoes'
        ELSE 'other'
    END AS endpoint_category
FROM request_log log
JOIN response_content content ON log.response_content_uuid = content.response_content_uuid
WHERE content.response_content IS NOT NULL
  AND content.response_content != ''
  AND TRY_CAST(content.response_content AS JSON) IS NOT NULL
{% if is_incremental() %}
  AND log.extracted_at > (SELECT MAX(extracted_at) FROM {{ this }})
{% endif %}