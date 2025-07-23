{{
  config(
    materialized='view'
  )
}}

SELECT
  c.anoContratacao,
  c.modalidadeNome,
  o.nomeOrgao,
  COUNT(c.numeroControlePNCP) AS quantidade_contratacoes,
  SUM(c.valorTotalEstimado) AS valor_total_estimado
FROM {{ ref('silver_contratacoes') }} c
LEFT JOIN {{ ref('silver_dim_unidades_orgao') }} o ON c.sequencialOrgao = o.sequencialOrgao
GROUP BY 1, 2, 3
