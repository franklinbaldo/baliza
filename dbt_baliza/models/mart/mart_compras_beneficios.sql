-- dbt_baliza/models/gold/mart_compras_beneficios.sql
{{
  config(
    materialized='incremental',
    unique_key='org_key',
    incremental_strategy='merge',
    description='Benefits and procurement impact analysis mart'
  )
}}

WITH beneficios_agregados AS (
    SELECT
        c.org_key,
        SUM(i.valor_total) AS valor_total_com_beneficio,
        COUNT(i.numero_item) AS qtd_itens_com_beneficio,
        (SELECT COUNT(*) FROM {{ ref('silver_itens_contratacao') }}) AS qtd_total_itens
    FROM {{ ref('silver_fact_contratacoes') }} c
    JOIN {{ ref('silver_itens_contratacao') }} i ON c.numero_controle_pncp = i.numero_controle_pncp
    WHERE i.modalidade_id IN (1, 2, 3)
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
