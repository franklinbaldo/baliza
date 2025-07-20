-- dbt_baliza/models/gold/mart_compras_beneficios.sql
{{
  config(
    materialized='incremental',
    unique_key='org_key',
    incremental_strategy='merge',
    description='Benefits and procurement impact analysis mart'
  )
}}

WITH itens_beneficios AS (
    SELECT
        i.numero_controle_pncp,
        i.valor_total,
        t.tipo_beneficio_id
    FROM {{ ref('silver_itens_contratacao') }} i
    JOIN {{ ref('tipos_beneficio') }} t ON i.tipo_beneficio_nome = t.tipo_beneficio_nome
),
beneficios_agregados AS (
    SELECT
        c.org_key,
        SUM(CASE WHEN ib.tipo_beneficio_id IN (1, 2, 3) THEN ib.valor_total ELSE 0 END) AS valor_total_com_beneficio,
        COUNT(CASE WHEN ib.tipo_beneficio_id IN (1, 2, 3) THEN 1 END) AS qtd_itens_com_beneficio,
        COUNT(*) AS qtd_total_itens
    FROM {{ ref('silver_fact_contratacoes') }} c
    JOIN itens_beneficios ib ON c.numero_controle_pncp = ib.numero_controle_pncp
    GROUP BY c.org_key
)
SELECT
    ba.org_key,
    ba.valor_total_com_beneficio,
    ba.qtd_itens_com_beneficio,
    ba.qtd_total_itens,
    (ba.qtd_itens_com_beneficio * 100.0 / ba.qtd_total_itens) AS percentual_itens_com_beneficio,
    (ba.valor_total_com_beneficio / pa.total_estimated_value) AS percentual_valor_com_beneficio
FROM beneficios_agregados ba
JOIN {{ ref('mart_procurement_analytics') }} pa ON ba.org_key = pa.org_key
{% if is_incremental() %}
WHERE ba.org_key NOT IN (SELECT org_key FROM {{ this }})
{% endif %}
