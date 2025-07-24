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
    WHERE endpoint_category = 'atas'
    {% if is_incremental() %}
    AND data_atualizacao > (SELECT MAX(data_atualizacao) FROM {{ this }})
    {% endif %}
)
SELECT
    id AS response_id,
    extracted_at,
    endpoint_name,
    endpoint_url,
    data_date,
    run_id,
    total_records,
    total_pages,
    current_page,

    -- Ata identifiers
    json_extract_string(response_json, '$.numeroControlePNCP') AS numero_controle_pncp,
    json_extract_string(response_json, '$.numeroAta') AS numero_ata,
    TRY_CAST(json_extract_string(response_json, '$.anoAta') AS INTEGER) AS ano_ata,

    -- Dates
    TRY_CAST(json_extract_string(response_json, '$.dataAssinatura') AS DATE) AS data_assinatura,
    TRY_CAST(json_extract_string(response_json, '$.dataVigenciaInicio') AS DATE) AS data_vigencia_inicio,
    TRY_CAST(json_extract_string(response_json, '$.dataVigenciaFim') AS DATE) AS data_vigencia_fim,
    TRY_CAST(json_extract_string(response_json, '$.dataPublicacaoPncp') AS TIMESTAMP) AS data_publicacao_pncp,
    TRY_CAST(json_extract_string(response_json, '$.dataAtualizacao') AS TIMESTAMP) AS data_atualizacao,

    -- Supplier information
    json_extract_string(response_json, '$.niFornecedor') AS ni_fornecedor,
    json_extract_string(response_json, '$.nomeRazaoSocialFornecedor') AS nome_razao_social_fornecedor,

    -- Ata details
    json_extract_string(response_json, '$.objetoAta') AS objeto_ata,
    json_extract_string(response_json, '$.informacaoComplementar') AS informacao_complementar,
    TRY_CAST(json_extract_string(response_json, '$.numeroRetificacao') AS INTEGER) AS numero_retificacao,

    -- Organization data (nested JSON)
    json_extract(response_json, '$.orgaoEntidade') AS orgao_entidade_json,
    json_extract(response_json, '$.unidadeOrgao') AS unidade_orgao_json

FROM source
WHERE rn = 1 AND json_extract_string(response_json, '$.numeroControlePNCP') IS NOT NULL