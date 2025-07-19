-- dbt_baliza/models/gold/mart_compras_beneficios.sql

{{
  config(
    materialized='incremental',
    unique_key='numero_controle_pncp',
    incremental_strategy='delete+insert',
    schema='gold'
  )
}}

WITH contratacoes AS (
  SELECT
    numero_controle_pncp,
    modalidade_nome,
    srp,
    valor_total_estimado,
    data_publicacao_pncp
  FROM {{ ref('silver_contratacoes') }}
  {% if is_incremental() %}
  WHERE data_publicacao_pncp > (SELECT MAX(data_publicacao_pncp) FROM {{ this }})
  {% endif %}
),

itens AS (
  SELECT
    numero_controle_pncp,
    tipo_beneficio_id,
    tipo_beneficio_nome,
    valor_total AS valor_total_item
  FROM {{ ref('silver_itens_contratacao') }}
),

-- Aggregate item data to get total value per benefit type for each procurement
beneficios_agregados AS (
  SELECT
    numero_controle_pncp,

    SUM(CASE WHEN tipo_beneficio_id IN (1, 2, 3) THEN valor_total_item ELSE 0 END) AS valor_total_com_beneficio,
    SUM(CASE WHEN tipo_beneficio_id NOT IN (1, 2, 3) THEN valor_total_item ELSE 0 END) AS valor_total_sem_beneficio,

    COUNT(CASE WHEN tipo_beneficio_id IN (1, 2, 3) THEN 1 END) AS qtd_itens_com_beneficio,
    COUNT(CASE WHEN tipo_beneficio_id NOT IN (1, 2, 3) THEN 1 END) AS qtd_itens_sem_beneficio,
    COUNT(*) AS qtd_total_itens

  FROM itens
  GROUP BY 1
),

-- Final mart model
final AS (
  SELECT
    c.numero_controle_pncp,
    c.modalidade_nome,
    c.srp,
    c.data_publicacao_pncp,

    COALESCE(b.qtd_total_itens, 0) AS quantidade_total_itens,
    COALESCE(b.qtd_itens_com_beneficio, 0) AS quantidade_itens_com_beneficio,
    COALESCE(b.qtd_itens_sem_beneficio, 0) AS quantidade_itens_sem_beneficio,

    c.valor_total_estimado,
    COALESCE(b.valor_total_com_beneficio, 0) AS valor_total_com_beneficio,
    COALESCE(b.valor_total_sem_beneficio, 0) AS valor_total_sem_beneficio,

    -- Calculated metrics
    CASE
      WHEN COALESCE(b.qtd_total_itens, 0) > 0
      THEN (COALESCE(b.qtd_itens_com_beneficio, 0) * 1.0 / b.qtd_total_itens)
      ELSE 0
    END AS percentual_itens_com_beneficio,

    CASE
      WHEN c.valor_total_estimado > 0
      THEN (COALESCE(b.valor_total_com_beneficio, 0) / c.valor_total_estimado)
      ELSE 0
    END AS percentual_valor_com_beneficio

  FROM contratacoes c
  LEFT JOIN beneficios_agregados b
    ON c.numero_controle_pncp = b.numero_controle_pncp
)

SELECT * FROM final
