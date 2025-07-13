-- Staging model: Clean and standardize raw contract data
{{ config(
    materialized='view',
    indexes=[
        {'columns': ['contrato_id'], 'unique': true},
        {'columns': ['data_assinatura']},
        {'columns': ['orgao_cnpj']},
        {'columns': ['uf_sigla']}
    ]
) }}

WITH contratos_cleaned AS (
    SELECT 
        -- Business keys and identifiers
        numeroControlePncpCompra AS contrato_id,
        COALESCE(baliza_run_id, ia_identifier) AS baliza_run_id,
        COALESCE(baliza_data_date, source_date) AS baliza_data_date,
        COALESCE(baliza_extracted_at, CURRENT_TIMESTAMP) AS baliza_extracted_at,
        
        -- Data source tracking
        data_source,
        ia_identifier,
        ia_file_name,
        
        -- Contract basic info
        numeroContratoEmpenho AS numero_contrato,
        anoContrato AS ano_contrato,
        
        -- Dates (convert from string to proper dates)
        TRY_CAST(dataAssinatura AS DATE) AS data_assinatura,
        TRY_CAST(dataVigenciaInicio AS DATE) AS data_vigencia_inicio,
        TRY_CAST(dataVigenciaFim AS DATE) AS data_vigencia_fim,
        
        -- Financial values (ensure proper numeric types)
        COALESCE(valorInicial, 0)::DECIMAL(18,2) AS valor_inicial_brl,
        COALESCE(valorParcela, 0)::DECIMAL(18,2) AS valor_parcela_brl,
        COALESCE(valorGlobal, 0)::DECIMAL(18,2) AS valor_global_brl,
        COALESCE(valorAcumulado, 0)::DECIMAL(18,2) AS valor_acumulado_brl,
        
        -- Supplier information
        niFornecedor AS fornecedor_ni,
        tipoPessoa AS fornecedor_tipo_pessoa,
        nomeRazaoSocialFornecedor AS fornecedor_nome,
        codigoPaisFornecedor AS fornecedor_pais_codigo,
        
        -- Contract object
        objetoContrato AS objeto_contrato,
        
        -- Extract structured data from JSON fields
        JSON_EXTRACT_STRING(tipoContrato_json, '$.nome') AS tipo_contrato_nome,
        JSON_EXTRACT_STRING(tipoContrato_json, '$.id') AS tipo_contrato_id,
        
        JSON_EXTRACT_STRING(orgaoEntidade_json, '$.cnpj') AS orgao_cnpj,
        JSON_EXTRACT_STRING(orgaoEntidade_json, '$.razaoSocial') AS orgao_razao_social,
        JSON_EXTRACT_STRING(orgaoEntidade_json, '$.poderId') AS orgao_poder_id,
        JSON_EXTRACT_STRING(orgaoEntidade_json, '$.esferaId') AS orgao_esfera_id,
        
        JSON_EXTRACT_STRING(categoriaProcesso_json, '$.nome') AS categoria_processo_nome,
        JSON_EXTRACT_STRING(categoriaProcesso_json, '$.id') AS categoria_processo_id,
        
        JSON_EXTRACT_STRING(unidadeOrgao_json, '$.ufNome') AS uf_nome,
        JSON_EXTRACT_STRING(unidadeOrgao_json, '$.ufSigla') AS uf_sigla,
        JSON_EXTRACT_STRING(unidadeOrgao_json, '$.municipioNome') AS municipio_nome,
        JSON_EXTRACT_STRING(unidadeOrgao_json, '$.codigoIbge') AS municipio_codigo_ibge,
        JSON_EXTRACT_STRING(unidadeOrgao_json, '$.nomeUnidade') AS unidade_nome,
        JSON_EXTRACT_STRING(unidadeOrgao_json, '$.codigoUnidade') AS unidade_codigo,
        
        -- Calculated fields
        CASE 
            WHEN TRY_CAST(dataVigenciaFim AS DATE) IS NOT NULL 
                 AND TRY_CAST(dataVigenciaInicio AS DATE) IS NOT NULL
            THEN DATE_DIFF('day', 
                          TRY_CAST(dataVigenciaInicio AS DATE), 
                          TRY_CAST(dataVigenciaFim AS DATE))
            ELSE NULL
        END AS duracao_contrato_dias,
        
        -- Basic data quality flags (for monitoring only)
        CASE 
            WHEN valorGlobal IS NULL OR valorGlobal <= 0 THEN TRUE 
            ELSE FALSE 
        END AS flag_valor_invalido,
        
        CASE 
            WHEN TRY_CAST(dataVigenciaInicio AS DATE) IS NULL 
                 OR TRY_CAST(dataVigenciaFim AS DATE) IS NULL
            THEN TRUE 
            ELSE FALSE 
        END AS flag_datas_incompletas,
        
        -- Raw JSON for complex analysis
        raw_data_json
        
    FROM {{ source('federated', 'contratos_unified') }}
    WHERE source_date >= '{{ var("start_date") }}'
)

SELECT * FROM contratos_cleaned