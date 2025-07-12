{{
  config(
    materialized='table',
    schema='marts'
  )
}}

WITH ref_entidades AS (
  SELECT
    cnpj_normalizado,
    razao_social,
    uf
  FROM {{ ref('ref_entidades_seed') }}
),

pncp_contratos_publicados AS (
  SELECT DISTINCT
    SUBSTR(orgao_cnpj, 1, 8) AS cnpj_normalizado -- orgao_cnpj is already cleaned in stg_contratos
  FROM {{ ref('stg_contratos') }}
  WHERE orgao_cnpj IS NOT NULL AND LENGTH(orgao_cnpj) >= 8 -- Ensure orgao_cnpj is valid for SUBSTR
)

SELECT
  r.cnpj_normalizado,
  r.razao_social,
  r.uf,
  CASE WHEN p.cnpj_normalizado IS NOT NULL THEN 1 ELSE 0 END AS publicou_alguma_vez
FROM ref_entidades r
LEFT JOIN pncp_contratos_publicados p
  ON r.cnpj_normalizado = p.cnpj_normalizado
ORDER BY r.uf, r.razao_social
