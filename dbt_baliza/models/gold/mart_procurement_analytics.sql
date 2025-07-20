{{
  config(
    materialized='incremental',
    unique_key='org_key',
    incremental_strategy='merge',
    description='Analytics mart combining procurement and contract data for business intelligence'
  )
}}

WITH procurement_analytics AS (
    SELECT
        org_key,
        COUNT(DISTINCT procurement_key) AS total_procurements,
        SUM(valor_total_estimado) AS total_estimated_value,
        AVG(duracao_proposta_dias) AS avg_proposal_duration
    FROM {{ ref('silver_fact_contratacoes') }}
    GROUP BY org_key
),
contract_analytics AS (
    SELECT
        org_key,
        COUNT(DISTINCT contract_key) AS total_contracts,
        SUM(valor_inicial) AS total_contract_value,
        AVG(duracao_vigencia_dias) AS avg_contract_duration
    FROM {{ ref('silver_fact_contratos') }}
    GROUP BY org_key
)
SELECT
    p.org_key,
    p.total_procurements,
    p.total_estimated_value,
    p.avg_proposal_duration,
    c.total_contracts,
    c.total_contract_value,
    c.avg_contract_duration
FROM procurement_analytics p
JOIN contract_analytics c ON p.org_key = c.org_key
{% if is_incremental() %}
WHERE p.org_key NOT IN (SELECT org_key FROM {{ this }})
{% endif %}