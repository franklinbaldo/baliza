{{
  config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert'
  )
}}

SELECT
    id,
    extracted_at,
    endpoint_name,
    endpoint_url,
    data_date,
    run_id,
    total_records,
    total_pages,
    current_page,
    TRY_CAST(response_content AS JSON) AS response_json
FROM {{ source('pncp', 'pncp_raw_responses') }}
WHERE endpoint_name IN ('contratacoes_publicacao', 'contratacoes_atualizacao', 'contratacoes_proposta')
  AND response_code = 200
  AND response_content IS NOT NULL
  AND response_content != ''
  AND TRY_CAST(response_content AS JSON) IS NOT NULL
{% if is_incremental() %}
  AND extracted_at > (SELECT MAX(extracted_at) FROM {{ this }})
{% endif %}
