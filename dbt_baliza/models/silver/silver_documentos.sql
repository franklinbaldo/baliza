{{
  config(
    materialized='incremental',
    unique_key='documento_key',
    incremental_strategy='merge'
  )
}}

{% set document_sources = {
    'contratacoes': 'documentos',
    'atas': 'documentos',
    'contratos': 'documentos'
} %}

WITH document_sources AS (
    {% for source_model, json_field in document_sources.items() %}
    SELECT
        numero_controle_pncp,
        '{{ source_model }}' AS tipo_referencia,
        data_atualizacao AS data_inclusao_referencia,
        json_extract(doc, '$') AS doc_data
    FROM {{ ref('silver_' ~ source_model) }},
    unnest(json_extract(response_json, '$.{{ json_field }}')) AS doc
    {% if is_incremental() %}
    WHERE data_atualizacao > (SELECT MAX(data_inclusao_referencia) FROM {{ this }} WHERE tipo_referencia = '{{ source_model }}')
    {% endif %}
    {{ "UNION ALL" if not loop.last }}
    {% endfor %}
),
documentos AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['numero_controle_pncp', "json_extract_string(doc_data, '$.id')"]) }} AS documento_key,
        numero_controle_pncp,
        tipo_referencia,
        data_inclusao_referencia,
        json_extract_string(doc_data, '$.titulo') AS titulo,
        json_extract_string(doc_data, '$.url') AS url,
        TRY_CAST(json_extract_string(doc_data, '$.data') AS TIMESTAMP) AS data_documento,
        TRY_CAST(json_extract_string(doc_data, '$.dataInclusao') AS TIMESTAMP) AS data_inclusao,
        TRY_CAST(json_extract_string(doc_data, '$.dataAtualizacao') AS TIMESTAMP) AS data_atualizacao,
        json_extract_string(doc_data, '$.tipoDocumentoNome') AS tipo_documento_nome
    FROM document_sources
)
SELECT * FROM documentos
