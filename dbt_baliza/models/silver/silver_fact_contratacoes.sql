{{
  config(
    materialized='incremental',
    unique_key='procurement_key',
    description='Fact table for procurements (contratações)',
    indexes=[
      {'columns': ['numero_controle_pncp'], 'unique': True},
      {'columns': ['data_publicacao_pncp']},
      {'columns': ['org_key']},
      {'columns': ['unit_key']}
    ]
  )
}}

WITH procurements AS (
  SELECT * FROM {{ ref('silver_contratacoes') }}
),
organizations AS (
  SELECT * FROM {{ ref('silver_dim_organizacoes') }}
),
units AS (
  SELECT * FROM {{ ref('silver_dim_unidades_orgao') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(['p.numero_controle_pncp']) }} AS procurement_key,
  p.numero_controle_pncp,
  p.data_publicacao_pncp,
  p.valor_total_estimado,
  p.objeto_compra,
  p.modalidade_nome,
  p.situacao_compra_nome,
  o.org_key,
  u.unit_key,
  p.data_atualizacao,
  {{ dbt_utils.datediff('p.data_abertura_proposta', 'p.data_encerramento_proposta', 'day') }} AS duracao_proposta_dias,
  CASE
    WHEN p.valor_total_estimado <= 10000 THEN 'Até 10k'
    WHEN p.valor_total_estimado <= 50000 THEN '10k-50k'
    WHEN p.valor_total_estimado <= 100000 THEN '50k-100k'
    ELSE 'Acima de 100k'
  END AS faixa_valor_estimado
FROM procurements p
LEFT JOIN organizations o ON json_extract_string(p.orgao_entidade_json, '$.cnpj') = o.cnpj
LEFT JOIN units u ON json_extract_string(p.unidade_orgao_json, '$.codigoUnidade') = u.codigo_unidade AND json_extract_string(p.orgao_entidade_json, '$.cnpj') = u.cnpj_orgao
{% if is_incremental() %}
WHERE p.data_atualizacao > (SELECT MAX(data_atualizacao) FROM {{ this }})
{% endif %}