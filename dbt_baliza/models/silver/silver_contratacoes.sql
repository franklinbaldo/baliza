{{
  config(
    materialized='incremental',
    unique_key='numero_controle_pncp',
    incremental_strategy='delete+insert'
  )
}}

WITH source AS (
    SELECT
        id,
        extracted_at,
        endpoint_name,
        endpoint_url,
        data_date,
        run_id,
        total_records,
        total_pages,
        current_page,
        response_json
    FROM {{ ref('bronze_contratacoes') }}
    {% if is_incremental() %}
    WHERE extracted_at > (SELECT MAX(extracted_at) FROM {{ this }})
    {% endif %}
),

-- Extract individual procurement records from the data array
procurement_records AS (
  SELECT
    source.id AS response_id,
    source.extracted_at,
    source.endpoint_name,
    source.endpoint_url,
    source.data_date,
    source.run_id,
    source.total_records,
    source.total_pages,
    source.current_page,
    -- Generate a unique key for each procurement record
    ROW_NUMBER() OVER (PARTITION BY source.id ORDER BY procurement_data_table.value) AS record_index,
    -- Extract individual procurement data
    procurement_data_table.value AS procurement_data
  FROM source
  CROSS JOIN json_each(json_extract(source.response_json, '$.data')) AS procurement_data_table
  WHERE json_extract(source.response_json, '$.data') IS NOT NULL
)

SELECT
  response_id,
  extracted_at,
  endpoint_name,
  endpoint_url,
  data_date,
  run_id,
  total_records,
  total_pages,
  current_page,
  record_index,
  
  -- Procurement identifiers
  procurement_data ->> 'numeroControlePNCP' AS numero_controle_pncp,
  procurement_data ->> 'numeroCompra' AS numero_compra,
  CAST(procurement_data ->> 'anoCompra' AS INTEGER) AS ano_compra,
  CAST(procurement_data ->> 'sequencialCompra' AS INTEGER) AS sequencial_compra,
  
  -- Dates
  TRY_CAST(procurement_data ->> 'dataPublicacaoPncp' AS TIMESTAMP) AS data_publicacao_pncp,
  TRY_CAST(procurement_data ->> 'dataAberturaProposta' AS TIMESTAMP) AS data_abertura_proposta,
  TRY_CAST(procurement_data ->> 'dataEncerramentoProposta' AS TIMESTAMP) AS data_encerramento_proposta,
  TRY_CAST(procurement_data ->> 'dataInclusao' AS TIMESTAMP) AS data_inclusao,
  TRY_CAST(procurement_data ->> 'dataAtualizacao' AS TIMESTAMP) AS data_atualizacao,
  TRY_CAST(procurement_data ->> 'dataAtualizacaoGlobal' AS TIMESTAMP) AS data_atualizacao_global,
  
  -- Amounts
  CAST(procurement_data ->> 'valorTotalEstimado' AS DOUBLE) AS valor_total_estimado,
  CAST(procurement_data ->> 'valorTotalHomologado' AS DOUBLE) AS valor_total_homologado,
  
  -- Procurement details
  procurement_data ->> 'objetoCompra' AS objeto_compra,
  procurement_data ->> 'informacaoComplementar' AS informacao_complementar,
  procurement_data ->> 'processo' AS processo,
  procurement_data ->> 'linkSistemaOrigem' AS link_sistema_origem,
  procurement_data ->> 'linkProcessoEletronico' AS link_processo_eletronico,
  procurement_data ->> 'justificativaPresencial' AS justificativa_presencial,
  
  -- Procurement method and mode
  CAST(procurement_data ->> 'modalidadeId' AS INTEGER) AS modalidade_id,
  procurement_data ->> 'modalidadeNome' AS modalidade_nome,
  CAST(procurement_data ->> 'modoDisputaId' AS INTEGER) AS modo_disputa_id,
  procurement_data ->> 'modoDisputaNome' AS modo_disputa_nome,
  
  -- Instrument and framework
  CAST(procurement_data ->> 'tipoInstrumentoConvocatorioCodigo' AS INTEGER) AS tipo_instrumento_convocatorio_codigo,
  procurement_data ->> 'tipoInstrumentoConvocatorioNome' AS tipo_instrumento_convocatorio_nome,
  
  -- Status and flags
  procurement_data ->> 'situacaoCompraId' AS situacao_compra_id,
  procurement_data ->> 'situacaoCompraNome' AS situacao_compra_nome,
  CAST(procurement_data ->> 'srp' AS BOOLEAN) AS srp,
  CAST(procurement_data ->> 'existeResultado' AS BOOLEAN) AS existe_resultado,
  
  -- Organization data (nested JSON)
  procurement_data -> 'orgaoEntidade' AS orgao_entidade_json,
  procurement_data -> 'unidadeOrgao' AS unidade_orgao_json,
  procurement_data -> 'orgaoSubRogado' AS orgao_subrogado_json,
  procurement_data -> 'unidadeSubRogada' AS unidade_subrogada_json,
  procurement_data -> 'amparoLegal' AS amparo_legal_json,
  procurement_data -> 'fontesOrcamentarias' AS fontes_orcamentarias_json,
  
  -- User information
  procurement_data ->> 'usuarioNome' AS usuario_nome,
  
  -- Full procurement data as JSON for fallback
  procurement_data AS procurement_json

FROM procurement_records