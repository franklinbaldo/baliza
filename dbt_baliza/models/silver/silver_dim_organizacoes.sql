{{
  config(
    materialized='incremental',
    unique_key='org_key',
    description='Dimension table for organizations (órgãos and entidades)'
  )
}}

WITH org_sources AS (
    SELECT orgao_entidade_json AS org_json FROM {{ ref('silver_contratos') }} WHERE orgao_entidade_json IS NOT NULL
    UNION ALL
    SELECT orgao_entidade_json FROM {{ ref('silver_contratacoes') }} WHERE orgao_entidade_json IS NOT NULL
    UNION ALL
    SELECT orgao_subrogado_json FROM {{ ref('silver_contratos') }} WHERE orgao_subrogado_json IS NOT NULL
    UNION ALL
    SELECT orgao_subrogado_json FROM {{ ref('silver_contratacoes') }} WHERE orgao_subrogado_json IS NOT NULL
),
organizations AS (
    SELECT DISTINCT
        json_extract_string(org_json, '$.cnpj') AS org_cnpj,
        json_extract_string(org_json, '$.razaoSocial') AS org_razao_social,
        json_extract_string(org_json, '$.poderId') AS org_poder_id,
        json_extract_string(org_json, '$.esferaId') AS org_esfera_id
    FROM org_sources
    WHERE json_extract_string(org_json, '$.cnpj') IS NOT NULL
)
SELECT
  {{ dbt_utils.generate_surrogate_key(['org_cnpj']) }} AS org_key,
  org_cnpj AS cnpj,
  org_razao_social AS razao_social,
  org_poder_id AS poder_id,
  org_esfera_id AS esfera_id,
  CASE
    WHEN org_poder_id = 'E' THEN 'Executivo'
    WHEN org_poder_id = 'L' THEN 'Legislativo'
    WHEN org_poder_id = 'J' THEN 'Judiciário'
    WHEN org_poder_id = 'M' THEN 'Ministério Público'
    ELSE 'Outros'
  END AS poder_nome,
  CASE
    WHEN org_esfera_id = 'F' THEN 'Federal'
    WHEN org_esfera_id = 'E' THEN 'Estadual'
    WHEN org_esfera_id = 'M' THEN 'Municipal'
    ELSE 'Outros'
  END AS esfera_nome,
  CURRENT_TIMESTAMP AS created_at,
  CURRENT_TIMESTAMP AS updated_at
FROM organizations
ORDER BY org_cnpj