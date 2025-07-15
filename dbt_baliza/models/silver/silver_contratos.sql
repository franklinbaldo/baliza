{{
  config(
    materialized='view',
    description='Silver layer for contracts, exploding JSON responses into individual records.'
  )
}}

WITH bronze_responses AS (
  SELECT *
  FROM {{ ref('bronze_pncp_responses') }}
  WHERE endpoint_name IN ('contratos_publicacao', 'contratos_atualizacao')
    AND response_code = 200
    AND response_content IS NOT NULL
    AND response_content != ''
),

parsed_responses AS (
  SELECT
    id,
    extracted_at,
    endpoint_name,
    endpoint_url,
    data_date,
    run_id,
    modalidade,
    total_records,
    total_pages,
    current_page,
    TRY_CAST(response_content AS JSON) AS response_json
  FROM bronze_responses
  WHERE TRY_CAST(response_content AS JSON) IS NOT NULL
)

SELECT
  p.id AS response_id,
  p.extracted_at,
  p.endpoint_name,
  p.endpoint_url,
  p.data_date,
  p.run_id,
  p.modalidade,
  p.total_records,
  p.total_pages,
  p.current_page,
  ROW_NUMBER() OVER (PARTITION BY p.id ORDER BY contract_data.value) AS record_index,
  contract_data.value AS contract_data
FROM parsed_responses AS p
CROSS JOIN json_each(json_extract(p.response_json, '$.data')) AS contract_data
WHERE json_extract(p.response_json, '$.data') IS NOT NULL
