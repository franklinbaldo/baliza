{{
  config(
    materialized='incremental',
    unique_key='unit_key',
    description='Dimension table for organizational units (unidades do órgão)'
  )
}}

WITH unit_sources AS (
    SELECT orgao_entidade_json, unidade_orgao_json FROM {{ ref('silver_contratos') }} WHERE orgao_entidade_json IS NOT NULL AND unidade_orgao_json IS NOT NULL
    UNION ALL
    SELECT orgao_entidade_json, unidade_orgao_json FROM {{ ref('silver_contratacoes') }} WHERE orgao_entidade_json IS NOT NULL AND unidade_orgao_json IS NOT NULL
    UNION ALL
    SELECT orgao_subrogado_json, unidade_subrogada_json FROM {{ ref('silver_contratos') }} WHERE orgao_subrogado_json IS NOT NULL AND unidade_subrogada_json IS NOT NULL
    UNION ALL
    SELECT orgao_subrogado_json, unidade_subrogada_json FROM {{ ref('silver_contratacoes') }} WHERE orgao_subrogado_json IS NOT NULL AND unidade_subrogada_json IS NOT NULL
),
units AS (
    SELECT DISTINCT
        json_extract_string(orgao_entidade_json, '$.cnpj') AS org_cnpj,
        json_extract_string(unidade_orgao_json, '$.codigoUnidade') AS unit_codigo_unidade,
        json_extract_string(unidade_orgao_json, '$.nomeUnidade') AS unit_nome_unidade,
        json_extract_string(unidade_orgao_json, '$.ufNome') AS unit_uf_nome,
        json_extract_string(unidade_orgao_json, '$.ufSigla') AS unit_uf_sigla,
        json_extract_string(unidade_orgao_json, '$.municipioNome') AS unit_municipio_nome,
        json_extract_string(unidade_orgao_json, '$.codigoIbge') AS unit_codigo_ibge
    FROM unit_sources
    WHERE json_extract_string(orgao_entidade_json, '$.cnpj') IS NOT NULL
      AND json_extract_string(unidade_orgao_json, '$.codigoUnidade') IS NOT NULL
)
SELECT
  {{ dbt_utils.generate_surrogate_key(['org_cnpj', 'unit_codigo_unidade']) }} AS unit_key,
  org_cnpj AS cnpj_orgao,
  unit_codigo_unidade AS codigo_unidade,
  unit_nome_unidade AS nome_unidade,
  unit_uf_nome AS uf_nome,
  unit_uf_sigla AS uf_sigla,
  unit_municipio_nome AS municipio_nome,
  unit_codigo_ibge AS codigo_ibge,
  CASE
    WHEN unit_uf_sigla IN ('AC', 'AM', 'AP', 'PA', 'RO', 'RR', 'TO') THEN 'Norte'
    WHEN unit_uf_sigla IN ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE') THEN 'Nordeste'
    WHEN unit_uf_sigla IN ('GO', 'MT', 'MS', 'DF') THEN 'Centro-Oeste'
    WHEN unit_uf_sigla IN ('ES', 'MG', 'RJ', 'SP') THEN 'Sudeste'
    WHEN unit_uf_sigla IN ('PR', 'RS', 'SC') THEN 'Sul'
    ELSE 'Outros'
  END AS regiao,
  CURRENT_TIMESTAMP AS created_at,
  CURRENT_TIMESTAMP AS updated_at
FROM units
ORDER BY org_cnpj, unit_codigo_unidade