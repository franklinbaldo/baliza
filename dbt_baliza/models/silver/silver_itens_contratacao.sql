-- dbt_baliza/models/silver/silver_itens_contratacao.sql

{{
  config(
    materialized='incremental',
    unique_key='item_key',
    incremental_strategy='delete+insert'
  )
}}

WITH source_contratacoes AS (
  SELECT
    procurement_json,
    numero_controle_pncp
  FROM {{ ref('silver_contratacoes') }}
  {% if is_incremental() %}
  -- this filter will be applied on an incremental run
  WHERE data_inclusao > (SELECT MAX(data_inclusao) FROM {{ this }})
  {% endif %}
),

-- Unnest the items from the procurement JSON data
unpacked_items AS (
  SELECT
    numero_controle_pncp,
    json_extract(item, '$') AS item_data,
    -- ROW_NUMBER() OVER (PARTITION BY numero_controle_pncp ORDER BY 1) AS item_sequence
  FROM source_contratacoes,
  unnest(json_extract(procurement_json, '$.itens')) AS item
),

-- Structure and clean the item data
final AS (
  SELECT
    -- Surrogate key for the item
    numero_controle_pncp || '-' || (item_data ->> 'numeroItem') AS item_key,

    -- Foreign key to the parent procurement
    numero_controle_pncp,

    -- Item details
    CAST(item_data ->> 'numeroItem' AS INTEGER) AS numero_item,
    item_data ->> 'descricao' AS descricao_item,
    CAST(item_data ->> 'quantidade' AS DOUBLE) AS quantidade,
    CAST(item_data ->> 'valorUnitarioEstimado' AS DOUBLE) AS valor_unitario_estimado,
    CAST(item_data ->> 'valorTotal' AS DOUBLE) AS valor_total,
    item_data ->> 'unidadeMedida' AS unidade_medida,

    -- Item classification
    item_data ->> 'tipoBeneficioId' AS tipo_beneficio_id,
    CASE CAST(item_data ->> 'tipoBeneficioId' AS INTEGER)
        WHEN 1 THEN 'Participação exclusiva para ME/EPP'
        WHEN 2 THEN 'Subcontratação para ME/EPP'
        WHEN 3 THEN 'Cota reservada para ME/EPP'
        WHEN 4 THEN 'Sem benefício'
        WHEN 5 THEN 'Não se aplica'
        ELSE 'Não especificado'
    END AS tipo_beneficio_nome,

    -- Item status
    item_data ->> 'situacaoCompraItemId' AS situacao_compra_item_id,
    CASE CAST(item_data ->> 'situacaoCompraItemId' AS INTEGER)
        WHEN 1 THEN 'Em Andamento'
        WHEN 2 THEN 'Homologado'
        WHEN 3 THEN 'Anulado/Revogado/Cancelado'
        WHEN 4 THEN 'Deserto'
        WHEN 5 THEN 'Fracassado'
        ELSE 'Não especificado'
    END AS situacao_compra_item_nome,

    -- Timestamps
    TRY_CAST(item_data ->> 'dataInclusao' AS TIMESTAMP) AS data_inclusao,
    TRY_CAST(item_data ->> 'dataAtualizacao' AS TIMESTAMP) AS data_atualizacao

  FROM unpacked_items
)

SELECT * FROM final
