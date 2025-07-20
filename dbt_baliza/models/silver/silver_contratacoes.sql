{{
  config(
    materialized='incremental',
    unique_key='numero_controle_pncp',
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
modalidades AS (
    SELECT * FROM {{ ref('modalidades_config') }}
)
SELECT
    s.id AS response_id,
    s.extracted_at,
    s.endpoint_name,
    s.endpoint_url,
    s.data_date,
    s.run_id,
    s.total_records,
    s.total_pages,
    s.current_page,

    -- Procurement identifiers
    json_extract_string(s.response_json, '$.numeroControlePNCP') AS numero_controle_pncp,
    json_extract_string(s.response_json, '$.numeroCompra') AS numero_compra,
    TRY_CAST(json_extract_string(s.response_json, '$.anoCompra') AS INTEGER) AS ano_compra,
    TRY_CAST(json_extract_string(s.response_json, '$.sequencialCompra') AS INTEGER) AS sequencial_compra,

    -- Dates
    TRY_CAST(json_extract_string(s.response_json, '$.dataPublicacaoPncp') AS TIMESTAMP) AS data_publicacao_pncp,
    TRY_CAST(json_extract_string(s.response_json, '$.dataAberturaProposta') AS TIMESTAMP) AS data_abertura_proposta,
    TRY_CAST(json_extract_string(s.response_json, '$.dataEncerramentoProposta') AS TIMESTAMP) AS data_encerramento_proposta,
    TRY_CAST(json_extract_string(s.response_json, '$.dataInclusao') AS TIMESTAMP) AS data_inclusao,
    TRY_CAST(json_extract_string(s.response_json, '$.dataAtualizacao') AS TIMESTAMP) AS data_atualizacao,

    -- Amounts
    TRY_CAST(json_extract_string(s.response_json, '$.valorTotalEstimado') AS DOUBLE) AS valor_total_estimado,

    -- Procurement details
    json_extract_string(s.response_json, '$.objetoCompra') AS objeto_compra,

    -- Procurement method and mode
    m.modalidade_nome,

    -- Status and flags
    json_extract_string(s.response_json, '$.situacaoCompraNome') AS situacao_compra_nome,
    TRY_CAST(json_extract_string(s.response_json, '$.srp') AS BOOLEAN) AS srp,

    -- Nested JSON
    json_extract(s.response_json, '$.orgaoEntidade') AS orgao_entidade_json,
    json_extract(s.response_json, '$.unidadeOrgao') AS unidade_orgao_json
FROM source s
LEFT JOIN modalidades m ON TRY_CAST(json_extract_string(s.response_json, '$.modalidadeId') AS INTEGER) = m.modalidade_id
WHERE s.rn = 1