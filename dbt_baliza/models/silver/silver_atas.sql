{{
  config(
    materialized='table'
  )
}}

WITH source AS (
    SELECT *
    FROM {{ ref('bronze_pncp_raw') }}
    WHERE endpoint_category = 'atas'
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
    response_json
  FROM source
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
  ata_data ->> 'numeroControlePNCP' AS numero_controle_pncp,
  ata_data ->> 'numeroAta' AS numero_ata,
  CAST(ata_data ->> 'anoAta' AS INTEGER) AS ano_ata,

  -- Dates
  TRY_CAST(ata_data ->> 'dataAssinatura' AS DATE) AS data_assinatura,
  TRY_CAST(ata_data ->> 'dataVigenciaInicio' AS DATE) AS data_vigencia_inicio,
  TRY_CAST(ata_data ->> 'dataVigenciaFim' AS DATE) AS data_vigencia_fim,
  TRY_CAST(ata_data ->> 'dataPublicacaoPncp' AS TIMESTAMP) AS data_publicacao_pncp,
  TRY_CAST(ata_data ->> 'dataAtualizacao' AS TIMESTAMP) AS data_atualizacao,

  -- Supplier information
  ata_data ->> 'niFornecedor' AS ni_fornecedor,
  ata_data ->> 'nomeRazaoSocialFornecedor' AS nome_razao_social_fornecedor,

  -- Ata details
  ata_data ->> 'objetoAta' AS objeto_ata,
  ata_data ->> 'informacaoComplementar' AS informacao_complementar,
  CAST(ata_data ->> 'numeroRetificacao' AS INTEGER) AS numero_retificacao,

  -- Organization data (nested JSON)
  ata_data -> 'orgaoEntidade' AS orgao_entidade_json,
  ata_data -> 'unidadeOrgao' AS unidade_orgao_json,

  -- Full ata data as JSON for fallback
  ata_data AS ata_json

FROM ata_records
WHERE ata_data ->> 'numeroControlePNCP' IS NOT NULL