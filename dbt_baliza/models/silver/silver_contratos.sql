{{
  config(
    materialized='incremental',
    unique_key='numero_controle_pncp',
    incremental_strategy='merge',
    on_schema_change='fail'
  )
}}

WITH source AS (
    SELECT *
    FROM {{ ref('stg_contratos') }}
    {% if is_incremental() %}
    WHERE extracted_at > (SELECT MAX(extracted_at) FROM {{ this }})
    {% endif %}
),

deduplicated_contracts AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY numero_controle_pncp
            ORDER BY data_atualizacao DESC
        ) AS rn
    FROM source
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
  
  -- Contract identifiers (already standardized in staging)
  numero_controle_pncp,
  numero_controle_pncp_compra,
  numero_contrato_empenho,
  ano_contrato,
  sequencial_contrato,
  
  -- Dates (already parsed in staging)
  data_assinatura,
  data_vigencia_inicio,
  data_vigencia_fim,
  data_publicacao_pncp,
  data_atualizacao,
  data_atualizacao_global,
  
  -- Amounts (already cast in staging)
  valor_inicial,
  valor_global,
  valor_parcela,
  valor_acumulado,
  
  -- Supplier information (standardized in staging)
  ni_fornecedor,
  tipo_pessoa,
  nome_razao_social_fornecedor,
  ni_fornecedor_subcontratado,
  nome_fornecedor_subcontratado,
  tipo_pessoa_subcontratada,
  
  -- Contract details (standardized in staging)
  objeto_contrato,
  informacao_complementar,
  processo,
  numero_parcelas,
  numero_retificacao,
  receita,
  
  -- Organization data (nested JSON) - preserved from staging
  orgao_entidade_json,
  unidade_orgao_json,
  orgao_subrogado_json,
  unidade_subrogada_json,
  tipo_contrato_json,
  categoria_processo_json,
  
  -- Additional identifiers (standardized in staging)
  codigo_pais_fornecedor,
  identificador_cipi,
  url_cipi,
  usuario_nome,
  
  -- Full contract data as JSON for fallback
  contract_json

FROM deduplicated_contracts
WHERE rn = 1