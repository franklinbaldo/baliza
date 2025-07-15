{{
  config(
    materialized='table',
    description='Dimension table for organizations (órgãos and entidades)'
  )
}}

WITH org_from_contracts AS (
  SELECT DISTINCT
    {{ extract_organization_data('orgao_entidade_json', 'org') }}
  FROM {{ ref('gld_contratos') }}
  WHERE orgao_entidade_json IS NOT NULL
),

org_from_procurements AS (
  SELECT DISTINCT
    {{ extract_organization_data('orgao_entidade_json', 'org') }}
  FROM {{ ref('gld_contratacoes') }}
  WHERE orgao_entidade_json IS NOT NULL
),

subrog_from_contracts AS (
  SELECT DISTINCT
    {{ extract_organization_data('orgao_subrogado_json', 'org') }}
  FROM {{ ref('gld_contratos') }}
  WHERE orgao_subrogado_json IS NOT NULL
),

subrog_from_procurements AS (
  SELECT DISTINCT
    {{ extract_organization_data('orgao_subrogado_json', 'org') }}
  FROM {{ ref('gld_contratacoes') }}
  WHERE orgao_subrogado_json IS NOT NULL
),

all_organizations AS (
  SELECT * FROM org_from_contracts
  UNION ALL
  SELECT * FROM org_from_procurements
  UNION ALL
  SELECT * FROM subrog_from_contracts
  UNION ALL
  SELECT * FROM subrog_from_procurements
),

deduplicated_organizations AS (
  SELECT DISTINCT
    org_cnpj,
    org_razao_social,
    org_poder_id,
    org_esfera_id
  FROM all_organizations
  WHERE org_cnpj IS NOT NULL
)

SELECT
  -- Surrogate key
  MD5(org_cnpj) AS org_key,
  
  -- Natural key
  org_cnpj AS cnpj,
  
  -- Organization details
  org_razao_social AS razao_social,
  org_poder_id AS poder_id,
  org_esfera_id AS esfera_id,
  
  -- Derived attributes
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
  
  -- Metadata
  CURRENT_TIMESTAMP AS created_at,
  CURRENT_TIMESTAMP AS updated_at

FROM deduplicated_organizations
ORDER BY org_cnpj