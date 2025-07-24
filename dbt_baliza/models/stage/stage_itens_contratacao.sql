-- dbt_baliza/models/silver/silver_itens_contratacao.sql
{{
  config(
    materialized='incremental',
    unique_key='item_key',
    incremental_strategy='merge'
  )
}}

WITH source AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY numero_controle_pncp ORDER BY data_atualizacao DESC) as rn
    FROM {{ ref('bronze_pncp_raw') }}
    WHERE endpoint_category = 'contratacoes'
    {% if is_incremental() %}
    AND data_atualizacao > (SELECT MAX(data_atualizacao) FROM {{ this }})
    {% endif %}
),
itens AS (
    SELECT
        json_extract_string(response_json, '$.numeroControlePNCP') as numero_controle_pncp,
        json_extract(item, '$') AS item_data
    FROM source,
    unnest(json_extract(response_json, '$.itens')) AS item
    WHERE rn = 1
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['numero_controle_pncp', "json_extract_string(item_data, '$.numeroItem')"]) }} AS item_key,
    numero_controle_pncp,
    TRY_CAST(json_extract_string(item_data, '$.numeroItem') AS INTEGER) AS numero_item,
    json_extract_string(item_data, '$.descricao') AS descricao_item,
    TRY_CAST(json_extract_string(item_data, '$.quantidade') AS DOUBLE) AS quantidade,
    TRY_CAST(json_extract_string(item_data, '$.valorUnitarioEstimado') AS DOUBLE) AS valor_unitario_estimado,
    TRY_CAST(json_extract_string(item_data, '$.valorTotal') AS DOUBLE) AS valor_total,
    json_extract_string(item_data, '$.unidadeMedida') AS unidade_medida,
    json_extract_string(item_data, '$.tipoBeneficioNome') AS tipo_beneficio_nome,
    json_extract_string(item_data, '$.situacaoCompraItemNome') AS situacao_compra_item_nome,
    TRY_CAST(json_extract_string(item_data, '$.dataInclusao') AS TIMESTAMP) AS data_inclusao,
    TRY_CAST(json_extract_string(item_data, '$.dataAtualizacao') AS TIMESTAMP) AS data_atualizacao
FROM itens
