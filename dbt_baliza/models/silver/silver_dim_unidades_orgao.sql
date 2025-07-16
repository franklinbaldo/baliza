{{
  config(
    materialized='table',
    description='Dimension table for organizational units (unidades do órgão)'
  )
}}

WITH units_from_contracts AS (
  SELECT DISTINCT
    {{ extract_organization_data('orgao_entidade_json', 'org') }},
    {{ extract_unit_data('unidade_orgao_json', 'unit') }}
  FROM {{ ref('silver_contratos') }}
  WHERE orgao_entidade_json IS NOT NULL
    AND unidade_orgao_json IS NOT NULL
),

units_from_procurements AS (
  SELECT DISTINCT
    {{ extract_organization_data('orgao_entidade_json', 'org') }},
    {{ extract_unit_data('unidade_orgao_json', 'unit') }}
  FROM {{ ref('silver_contratacoes') }}
  WHERE orgao_entidade_json IS NOT NULL
    AND unidade_orgao_json IS NOT NULL
),

subrog_units_from_contracts AS (
  SELECT DISTINCT
    {{ extract_organization_data('orgao_subrogado_json', 'org') }},
    {{ extract_unit_data('unidade_subrogada_json', 'unit') }}
  FROM {{ ref('silver_contratos') }}
  WHERE orgao_subrogado_json IS NOT NULL
    AND unidade_subrogada_json IS NOT NULL
),

subrog_units_from_procurements AS (
  SELECT DISTINCT
    {{ extract_organization_data('orgao_subrogado_json', 'org') }},
    {{ extract_unit_data('unidade_subrogada_json', 'unit') }}
  FROM {{ ref('silver_contratacoes') }}
  WHERE orgao_subrogado_json IS NOT NULL
    AND unidade_subrogada_json IS NOT NULL
),

all_units AS (
  SELECT * FROM units_from_contracts
  UNION ALL
  SELECT * FROM units_from_procurements
  UNION ALL
  SELECT * FROM subrog_units_from_contracts
  UNION ALL
  SELECT * FROM subrog_units_from_procurements
),

deduplicated_units AS (
  SELECT DISTINCT
    org_cnpj,
    unit_codigo_unidade,
    unit_nome_unidade,
    unit_uf_nome,
    unit_uf_sigla,
    unit_municipio_nome,
    unit_codigo_ibge
  FROM all_units
  WHERE org_cnpj IS NOT NULL
    AND unit_codigo_unidade IS NOT NULL
)

SELECT
  -- Surrogate key
  MD5(org_cnpj || '|' || unit_codigo_unidade) AS unit_key,
  
  -- Natural keys
  org_cnpj AS cnpj_orgao,
  unit_codigo_unidade AS codigo_unidade,
  
  -- Unit details
  unit_nome_unidade AS nome_unidade,
  unit_uf_nome AS uf_nome,
  unit_uf_sigla AS uf_sigla,
  unit_municipio_nome AS municipio_nome,
  unit_codigo_ibge AS codigo_ibge,
  
  -- Derived attributes
  CASE 
    WHEN unit_uf_sigla IN ('AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO') THEN unit_uf_sigla
    ELSE 'OUTROS'
  END AS uf_sigla_normalizada,
  
  CASE 
    WHEN unit_uf_sigla IN ('AC', 'AM', 'AP', 'PA', 'RO', 'RR', 'TO') THEN 'Norte'
    WHEN unit_uf_sigla IN ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE') THEN 'Nordeste'
    WHEN unit_uf_sigla IN ('GO', 'MT', 'MS', 'DF') THEN 'Centro-Oeste'
    WHEN unit_uf_sigla IN ('ES', 'MG', 'RJ', 'SP') THEN 'Sudeste'
    WHEN unit_uf_sigla IN ('PR', 'RS', 'SC') THEN 'Sul'
    ELSE 'Outros'
  END AS regiao,
  
  -- Metadata
  CURRENT_TIMESTAMP AS created_at,
  CURRENT_TIMESTAMP AS updated_at

FROM deduplicated_units
ORDER BY org_cnpj, unit_codigo_unidade