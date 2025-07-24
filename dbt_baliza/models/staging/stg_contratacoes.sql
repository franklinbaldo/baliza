{{
  config(
    materialized='view'
  )
}}

WITH source AS (
    SELECT *
    FROM {{ ref('bronze_pncp_raw') }}
    WHERE endpoint_category = 'contratacoes'
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

-- Extract individual contratacao records from the data array
contratacao_records AS (
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
    -- Generate a unique key for each contratacao record
    ROW_NUMBER() OVER (PARTITION BY parsed_responses.id ORDER BY contratacao_data_table.value) AS record_index,
    -- Extract individual contratacao data
    contratacao_data_table.value AS contratacao_data
  FROM parsed_responses
  CROSS JOIN json_each(json_extract(parsed_responses.response_json, '$.data')) AS contratacao_data_table
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
  contratacao_data ->> 'numeroControlePNCP' AS numero_controle_pncp,
  contratacao_data ->> 'numeroControlePncpCompra' AS numero_controle_pncp_compra,
  contratacao_data ->> 'numeroContratacao' AS numero_contratacao,

  -- Type casting with proper handling
  CAST(contratacao_data ->> 'anoContratacao' AS INTEGER) AS ano_contratacao,
  CAST(contratacao_data ->> 'sequencialContratacao' AS INTEGER) AS sequencial_contratacao,

  -- Date standardization
  TRY_CAST(contratacao_data ->> 'dataAssinatura' AS DATE) AS data_assinatura,
  TRY_CAST(contratacao_data ->> 'dataVigenciaInicio' AS DATE) AS data_vigencia_inicio,
  TRY_CAST(contratacao_data ->> 'dataVigenciaFim' AS DATE) AS data_vigencia_fim,
  TRY_CAST(contratacao_data ->> 'dataPublicacaoPncp' AS TIMESTAMP) AS data_publicacao_pncp,
  TRY_CAST(contratacao_data ->> 'dataAtualizacao' AS TIMESTAMP) AS data_atualizacao,

  -- Monetary values with proper decimal precision
  CAST(contratacao_data ->> 'valorInicial' AS DECIMAL(15,2)) AS valor_inicial,
  CAST(contratacao_data ->> 'valorGlobal' AS DECIMAL(15,2)) AS valor_global,

  -- Supplier information standardization
  contratacao_data ->> 'niFornecedor' AS ni_fornecedor,
  contratacao_data ->> 'tipoPessoa' AS tipo_pessoa,
  contratacao_data ->> 'nomeRazaoSocialFornecedor' AS nome_razao_social_fornecedor,

  -- Contratacao details
  contratacao_data ->> 'objetoContratacao' AS objeto_contratacao,
  contratacao_data ->> 'informacaoComplementar' AS informacao_complementar,
  contratacao_data ->> 'processo' AS processo,
  CAST(contratacao_data ->> 'numeroRetificacao' AS INTEGER) AS numero_retificacao,
  CAST(contratacao_data ->> 'receita' AS BOOLEAN) AS receita,

  -- Organization data (nested JSON) - preserved for silver layer
  contratacao_data -> 'orgaoEntidade' AS orgao_entidade_json,
  contratacao_data -> 'unidadeOrgao' AS unidade_orgao_json,
  contratacao_data -> 'tipoContratacao' AS tipo_contratacao_json,
  contratacao_data -> 'categoriaProcesso' AS categoria_processo_json,

  -- Additional identifiers
  contratacao_data ->> 'codigoPaisFornecedor' AS codigo_pais_fornecedor,
  contratacao_data ->> 'identificadorCipi' AS identificador_cipi,
  contratacao_data ->> 'urlCipi' AS url_cipi,
  contratacao_data ->> 'usuarioNome' AS usuario_nome,

  -- Full contratacao data as JSON for fallback
  contratacao_data AS contratacao_json

FROM contratacao_records