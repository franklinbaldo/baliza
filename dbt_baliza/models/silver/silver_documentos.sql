-- dbt_baliza/models/silver/silver_documentos.sql

{{
  config(
    materialized='incremental',
    unique_key='documento_key',
    incremental_strategy='delete+insert'
  )
}}

WITH source_contratacoes AS (
  SELECT
    numero_controle_pncp,
    procurement_json,
    data_inclusao
  FROM {{ ref('silver_contratacoes') }}
  {% if is_incremental() %}
  WHERE data_inclusao > (SELECT MAX(data_inclusao_referencia) FROM {{ this }} WHERE tipo_referencia = 'contratacao')
  {% endif %}
),

-- Adicionar outras fontes (atas, contratos) aqui no futuro
-- source_atas AS ( ... ),
-- source_contratos AS ( ... ),

unpacked_docs AS (
  -- Documentos de Contratações
  SELECT
    numero_controle_pncp,
    'contratacao' AS tipo_referencia,
    data_inclusao AS data_inclusao_referencia,
    json_extract(doc, '$') AS doc_data
  FROM source_contratacoes,
  unnest(json_extract(procurement_json, '$.documentos')) AS doc

  -- UNION ALL para outras fontes no futuro
),

final AS (
  SELECT
    -- Surrogate key for the document
    doc_data ->> 'id' AS documento_key,

    -- Foreign key and reference type
    numero_controle_pncp,
    tipo_referencia,
    data_inclusao_referencia,

    -- Document details
    CAST(doc_data ->> 'tipoDocumentoId' AS INTEGER) AS tipo_documento_id,
    CASE CAST(doc_data ->> 'tipoDocumentoId' AS INTEGER)
      WHEN 1 THEN 'Aviso de Contratação Direta'
      WHEN 2 THEN 'Edital'
      WHEN 3 THEN 'Minuta do Contrato'
      WHEN 4 THEN 'Termo de Referência'
      WHEN 5 THEN 'Anteprojeto'
      WHEN 6 THEN 'Projeto Básico'
      WHEN 7 THEN 'Estudo Técnico Preliminar'
      WHEN 8 THEN 'Projeto Executivo'
      WHEN 9 THEN 'Mapa de Riscos'
      WHEN 10 THEN 'DFD'
      WHEN 11 THEN 'Ata de Registro de Preço'
      WHEN 12 THEN 'Contrato'
      WHEN 13 THEN 'Termo de Rescisão'
      WHEN 14 THEN 'Termo Aditivo'
      WHEN 15 THEN 'Termo de Apostilamento'
      WHEN 16 THEN 'Outros'
      WHEN 17 THEN 'Nota de Empenho'
      WHEN 18 THEN 'Relatório Final de Contrato'
      ELSE 'Não especificado'
    END AS tipo_documento_nome,
    doc_data ->> 'titulo' AS titulo,
    doc_data ->> 'url' AS url,
    TRY_CAST(doc_data ->> 'data' AS TIMESTAMP) AS data_documento,

    -- Timestamps
    TRY_CAST(doc_data ->> 'dataInclusao' AS TIMESTAMP) AS data_inclusao,
    TRY_CAST(doc_data ->> 'dataAtualizacao' AS TIMESTAMP) AS data_atualizacao

  FROM unpacked_docs
)

SELECT * FROM final
