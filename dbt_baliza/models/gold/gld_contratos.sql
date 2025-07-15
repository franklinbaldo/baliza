{{
  config(
    materialized='view',
    description='Gold layer for contracts, selecting and casting columns from the silver layer.'
  )
}}

WITH silver_contracts AS (
  SELECT *
  FROM {{ ref('silver_contratos') }}
)

SELECT
  response_id,
  extracted_at,
  endpoint_name,
  endpoint_url,
  data_date,
  run_id,
  modalidade,
  total_records,
  total_pages,
  current_page,
  record_index,

  -- Contract identifiers
  contract_data ->> 'numeroControlePNCP' AS numero_controle_pncp,

  -- Dates
  TRY_CAST(contract_data ->> 'dataAssinatura' AS TIMESTAMP) AS data_assinatura,
  TRY_CAST(contract_data ->> 'dataPublicacaoPncp' AS TIMESTAMP) AS data_publicacao_pncp,
  TRY_CAST(contract_data ->> 'dataInicioVigencia' AS TIMESTAMP) AS data_inicio_vigencia,
  TRY_CAST(contract_data ->> 'dataFimVigencia' AS TIMESTAMP) AS data_fim_vigencia,

  -- Amounts
  CAST(contract_data ->> 'valorInicial' AS DOUBLE) AS valor_inicial,
  CAST(contract_data ->> 'valorGlobal' AS DOUBLE) AS valor_global,

  -- Contract details
  contract_data ->> 'objetoContrato' AS objeto_contrato,
  contract_data ->> 'informacaoComplementar' AS informacao_complementar,

  -- Organization data (nested JSON)
  contract_data -> 'compra' AS compra_json,
  contract_data -> 'fornecedor' AS fornecedor_json,

  -- Full contract data as JSON for fallback
  contract_data AS contract_json

FROM silver_contracts
