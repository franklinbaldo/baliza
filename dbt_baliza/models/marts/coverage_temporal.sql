{{
  config(
    materialized='table',
    schema='marts'
  )
}}

WITH ref_entidades AS (
  SELECT
    cnpj_normalizado,
    razao_social, -- Included for potential future use, not strictly needed for this model's output
    uf            -- Included for potential future use
  FROM {{ ref('ref_entidades_seed') }}
),

pncp_contratos AS (
  SELECT
    SUBSTR(orgao_cnpj, 1, 8) AS cnpj_normalizado, -- orgao_cnpj is already cleaned in stg_contratos
    data_assinatura AS data_publicacao           -- data_assinatura is already a DATE in stg_contratos
  FROM {{ ref('stg_contratos') }}
  WHERE data_assinatura IS NOT NULL
    AND orgao_cnpj IS NOT NULL AND LENGTH(orgao_cnpj) >= 8 -- Ensure orgao_cnpj is valid for SUBSTR
),

dias_referencia AS (
  -- Generate a series of days from a fixed start date (PNCP launch) to current date for each CNPJ
  SELECT
    e.cnpj_normalizado,
    d.dia::date AS dia
  FROM ref_entidades e
  CROSS JOIN {{ dbt_utils.date_spine(
                datepart="day",
                start_date="'2021-04-01'",
                end_date="current_date"
             ) }} AS d
),

contratos_por_dia AS (
  SELECT
    cnpj_normalizado,
    data_publicacao,
    COUNT(*) AS num_contratos_dia -- In case there are multiple contracts on the same day for the same CNPJ
  FROM pncp_contratos
  GROUP BY 1, 2
)

SELECT
  dr.cnpj_normalizado,
  COUNT(DISTINCT cpd.data_publicacao)                         AS dias_publicados,
  COUNT(DISTINCT dr.dia)                                      AS dias_totais, -- Should be the same for all CNPJs if date spine is correct
  ROUND(100.0 * COUNT(DISTINCT cpd.data_publicacao) / NULLIF(COUNT(DISTINCT dr.dia), 0), 1) AS pct_cobertura_dia
FROM dias_referencia dr
LEFT JOIN contratos_por_dia cpd
  ON dr.cnpj_normalizado = cpd.cnpj_normalizado
  AND dr.dia = cpd.data_publicacao
GROUP BY 1
ORDER BY dr.cnpj_normalizado
