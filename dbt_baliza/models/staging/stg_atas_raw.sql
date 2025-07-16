{{
  config(
    materialized='view',
    description='Staged raw price registration records (atas) data from PNCP API responses'
  )
}}

WITH raw_responses AS (
  SELECT *
  FROM main_psa.pncp_raw_responses
  WHERE endpoint_name IN ('atas_periodo', 'atas_atualizacao')
    AND response_code = 200
    AND response_content IS NOT NULL
    AND response_content != ''
),

parsed_responses AS (
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
    -- Parse the JSON response content
    TRY_CAST(response_content AS JSON) AS response_json
  FROM raw_responses
  WHERE TRY_CAST(response_content AS JSON) IS NOT NULL
),

-- Extract individual ata records from the data array
ata_records AS (
  SELECT
    parsed_responses.id AS response_id,
    parsed_responses.extracted_at,
    parsed_responses.endpoint_name,
    parsed_responses.endpoint_url,
    parsed_responses.data_date,
    parsed_responses.run_id,
    parsed_responses.total_records,
    parsed_responses.total_pages,
    parsed_responses.current_page,
    -- Generate a unique key for each ata record
    ROW_NUMBER() OVER (PARTITION BY parsed_responses.id ORDER BY ata_data_table.value) AS record_index,
    -- Extract individual ata data
    ata_data_table.value AS ata_data
  FROM parsed_responses
  CROSS JOIN json_each(json_extract(parsed_responses.response_json, '$.data')) AS ata_data_table
  WHERE json_extract(parsed_responses.response_json, '$.data') IS NOT NULL
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
  
  -- Ata identifiers
  ata_data ->> 'numeroControlePNCPAta' AS numero_controle_pncp_ata,
  ata_data ->> 'numeroAtaRegistroPreco' AS numero_ata_registro_preco,
  CAST(ata_data ->> 'anoAta' AS INTEGER) AS ano_ata,
  ata_data ->> 'numeroControlePNCPCompra' AS numero_controle_pncp_compra,
  
  -- Dates
  TRY_CAST(ata_data ->> 'dataAssinatura' AS TIMESTAMP) AS data_assinatura,
  TRY_CAST(ata_data ->> 'vigenciaInicio' AS TIMESTAMP) AS vigencia_inicio,
  TRY_CAST(ata_data ->> 'vigenciaFim' AS TIMESTAMP) AS vigencia_fim,
  TRY_CAST(ata_data ->> 'dataPublicacaoPncp' AS TIMESTAMP) AS data_publicacao_pncp,
  TRY_CAST(ata_data ->> 'dataInclusao' AS TIMESTAMP) AS data_inclusao,
  TRY_CAST(ata_data ->> 'dataAtualizacao' AS TIMESTAMP) AS data_atualizacao,
  TRY_CAST(ata_data ->> 'dataAtualizacaoGlobal' AS TIMESTAMP) AS data_atualizacao_global,
  TRY_CAST(ata_data ->> 'dataCancelamento' AS TIMESTAMP) AS data_cancelamento,
  
  -- Status and flags
  CAST(ata_data ->> 'cancelado' AS BOOLEAN) AS cancelado,
  
  -- Ata details
  ata_data ->> 'objetoContratacao' AS objeto_contratacao,
  ata_data ->> 'usuario' AS usuario,
  
  -- Organization information (main organ)
  ata_data ->> 'cnpjOrgao' AS cnpj_orgao,
  ata_data ->> 'nomeOrgao' AS nome_orgao,
  ata_data ->> 'codigoUnidadeOrgao' AS codigo_unidade_orgao,
  ata_data ->> 'nomeUnidadeOrgao' AS nome_unidade_orgao,
  
  -- Subrogated organization information
  ata_data ->> 'cnpjOrgaoSubrogado' AS cnpj_orgao_subrogado,
  ata_data ->> 'nomeOrgaoSubrogado' AS nome_orgao_subrogado,
  ata_data ->> 'codigoUnidadeOrgaoSubrogado' AS codigo_unidade_orgao_subrogado,
  ata_data ->> 'nomeUnidadeOrgaoSubrogado' AS nome_unidade_orgao_subrogado,
  
  -- Full ata data as JSON for fallback
  ata_data AS ata_json

FROM ata_records