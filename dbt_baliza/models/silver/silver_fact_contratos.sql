{{
  config(
    materialized='incremental',
    unique_key='contract_key',
    description='Fact table for contracts (contratos/empenhos)',
    indexes=[
      {'columns': ['numero_controle_pncp'], 'unique': True},
      {'columns': ['data_assinatura']},
      {'columns': ['org_key']},
      {'columns': ['unit_key']}
    ]
  )
}}

WITH contracts AS (
  SELECT * FROM {{ ref('silver_contratos') }}
),
organizations AS (
  SELECT * FROM {{ ref('silver_dim_organizacoes') }}
),
units AS (
  SELECT * FROM {{ ref('silver_dim_unidades_orgao') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(['c.numero_controle_pncp']) }} AS contract_key,
  c.numero_controle_pncp,
  c.data_assinatura,
  c.data_vigencia_inicio,
  c.data_vigencia_fim,
  c.valor_inicial,
  c.ni_fornecedor,
  c.nome_razao_social_fornecedor,
  c.objeto_contrato,
  o.org_key,
  u.unit_key,
  c.data_atualizacao,
  {{ dbt_utils.datediff('c.data_vigencia_inicio', 'c.data_vigencia_fim', 'day') }} AS duracao_vigencia_dias,
  CASE
    WHEN c.valor_inicial <= 10000 THEN 'Até 10k'
    WHEN c.valor_inicial <= 50000 THEN '10k-50k'
    WHEN c.valor_inicial <= 100000 THEN '50k-100k'
    ELSE 'Acima de 100k'
  END AS faixa_valor,
  {{ dbt_utils.generate_surrogate_key(['c.ni_fornecedor']) }} AS fornecedor_key
FROM contracts c
LEFT JOIN organizations o ON json_extract_string(c.orgao_entidade_json, '$.cnpj') = o.cnpj
LEFT JOIN units u ON json_extract_string(c.unidade_orgao_json, '$.codigoUnidade') = u.codigo_unidade AND json_extract_string(c.orgao_entidade_json, '$.cnpj') = u.cnpj_orgao
{% if is_incremental() %}
WHERE c.data_atualizacao > (SELECT MAX(data_atualizacao) FROM {{ this }})
{% endif %}