{{
  config(
    materialized='view'
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
  
  -- Standardized column names (snake_case)
  ata_data ->> 'numeroControlePNCP' AS numero_controle_pncp,
  ata_data ->> 'numeroControlePncpCompra' AS numero_controle_pncp_compra,
  ata_data ->> 'numeroSeqencialAta' AS numero_sequencial_ata,
  
  -- Type casting with proper handling
  CAST(ata_data ->> 'anoAta' AS INTEGER) AS ano_ata,
  CAST(ata_data ->> 'sequencialAta' AS INTEGER) AS sequencial_ata,
  
  -- Date standardization
  TRY_CAST(ata_data ->> 'dataAssinatura' AS DATE) AS data_assinatura,
  TRY_CAST(ata_data ->> 'dataVigenciaInicio' AS DATE) AS data_vigencia_inicio,
  TRY_CAST(ata_data ->> 'dataVigenciaFim' AS DATE) AS data_vigencia_fim,
  TRY_CAST(ata_data ->> 'dataPublicacaoPncp' AS TIMESTAMP) AS data_publicacao_pncp,
  TRY_CAST(ata_data ->> 'dataAtualizacao' AS TIMESTAMP) AS data_atualizacao,
  
  -- Monetary values with proper decimal precision
  CAST(ata_data ->> 'valorInicial' AS DECIMAL(15,2)) AS valor_inicial,
  CAST(ata_data ->> 'valorGlobal' AS DECIMAL(15,2)) AS valor_global,
  
  -- Supplier information standardization
  ata_data ->> 'niFornecedor' AS ni_fornecedor,
  ata_data ->> 'tipoPessoa' AS tipo_pessoa,
  ata_data ->> 'nomeRazaoSocialFornecedor' AS nome_razao_social_fornecedor,
  
  -- Ata details
  ata_data ->> 'objetoAta' AS objeto_ata,
  ata_data ->> 'informacaoComplementar' AS informacao_complementar,
  ata_data ->> 'processo' AS processo,
  CAST(ata_data ->> 'numeroRetificacao' AS INTEGER) AS numero_retificacao,
  CAST(ata_data ->> 'receita' AS BOOLEAN) AS receita,
  
  -- Organization data (nested JSON) - preserved for silver layer
  ata_data -> 'orgaoEntidade' AS orgao_entidade_json,
  ata_data -> 'unidadeOrgao' AS unidade_orgao_json,
  ata_data -> 'tipoAta' AS tipo_ata_json,
  ata_data -> 'categoriaProcesso' AS categoria_processo_json,
  
  -- Additional identifiers
  ata_data ->> 'codigoPaisFornecedor' AS codigo_pais_fornecedor,
  ata_data ->> 'identificadorCipi' AS identificador_cipi,
  ata_data ->> 'urlCipi' AS url_cipi,
  ata_data ->> 'usuarioNome' AS usuario_nome,
  
  -- Full ata data as JSON for fallback
  ata_data AS ata_json

FROM ata_records