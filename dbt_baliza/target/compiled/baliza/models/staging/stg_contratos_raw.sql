

WITH raw_responses AS (
  SELECT *
  FROM "baliza"."psa"."pncp_raw_responses"
  WHERE endpoint_name IN ('contratos_publicacao', 'contratos_atualizacao')
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

-- Extract individual contract records from the data array
contract_records AS (
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
    -- Generate a unique key for each contract record
    ROW_NUMBER() OVER (PARTITION BY parsed_responses.id ORDER BY contract_data_table.value) AS record_index,
    -- Extract individual contract data
    contract_data_table.value AS contract_data
  FROM parsed_responses
  CROSS JOIN json_each(json_extract(parsed_responses.response_json, '$.data')) AS contract_data_table
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
  
  -- Contract identifiers
  contract_data ->> 'numeroControlePNCP' AS numero_controle_pncp,
  contract_data ->> 'numeroControlePncpCompra' AS numero_controle_pncp_compra,
  contract_data ->> 'numeroContratoEmpenho' AS numero_contrato_empenho,
  CAST(contract_data ->> 'anoContrato' AS INTEGER) AS ano_contrato,
  CAST(contract_data ->> 'sequencialContrato' AS INTEGER) AS sequencial_contrato,
  
  -- Dates
  TRY_CAST(contract_data ->> 'dataAssinatura' AS DATE) AS data_assinatura,
  TRY_CAST(contract_data ->> 'dataVigenciaInicio' AS DATE) AS data_vigencia_inicio,
  TRY_CAST(contract_data ->> 'dataVigenciaFim' AS DATE) AS data_vigencia_fim,
  TRY_CAST(contract_data ->> 'dataPublicacaoPncp' AS TIMESTAMP) AS data_publicacao_pncp,
  TRY_CAST(contract_data ->> 'dataAtualizacao' AS TIMESTAMP) AS data_atualizacao,
  TRY_CAST(contract_data ->> 'dataAtualizacaoGlobal' AS TIMESTAMP) AS data_atualizacao_global,
  
  -- Amounts
  CAST(contract_data ->> 'valorInicial' AS DOUBLE) AS valor_inicial,
  CAST(contract_data ->> 'valorGlobal' AS DOUBLE) AS valor_global,
  CAST(contract_data ->> 'valorParcela' AS DOUBLE) AS valor_parcela,
  CAST(contract_data ->> 'valorAcumulado' AS DOUBLE) AS valor_acumulado,
  
  -- Supplier information
  contract_data ->> 'niFornecedor' AS ni_fornecedor,
  contract_data ->> 'tipoPessoa' AS tipo_pessoa,
  contract_data ->> 'nomeRazaoSocialFornecedor' AS nome_razao_social_fornecedor,
  contract_data ->> 'niFornecedorSubContratado' AS ni_fornecedor_subcontratado,
  contract_data ->> 'nomeFornecedorSubContratado' AS nome_fornecedor_subcontratado,
  contract_data ->> 'tipoPessoaSubContratada' AS tipo_pessoa_subcontratada,
  
  -- Contract details
  contract_data ->> 'objetoContrato' AS objeto_contrato,
  contract_data ->> 'informacaoComplementar' AS informacao_complementar,
  contract_data ->> 'processo' AS processo,
  CAST(contract_data ->> 'numeroParcelas' AS INTEGER) AS numero_parcelas,
  CAST(contract_data ->> 'numeroRetificacao' AS INTEGER) AS numero_retificacao,
  CAST(contract_data ->> 'receita' AS BOOLEAN) AS receita,
  
  -- Organization data (nested JSON)
  contract_data -> 'orgaoEntidade' AS orgao_entidade_json,
  contract_data -> 'unidadeOrgao' AS unidade_orgao_json,
  contract_data -> 'orgaoSubRogado' AS orgao_subrogado_json,
  contract_data -> 'unidadeSubRogada' AS unidade_subrogada_json,
  contract_data -> 'tipoContrato' AS tipo_contrato_json,
  contract_data -> 'categoriaProcesso' AS categoria_processo_json,
  
  -- Additional identifiers
  contract_data ->> 'codigoPaisFornecedor' AS codigo_pais_fornecedor,
  contract_data ->> 'identificadorCipi' AS identificador_cipi,
  contract_data ->> 'urlCipi' AS url_cipi,
  contract_data ->> 'usuarioNome' AS usuario_nome,
  
  -- Full contract data as JSON for fallback
  contract_data AS contract_json

FROM contract_records