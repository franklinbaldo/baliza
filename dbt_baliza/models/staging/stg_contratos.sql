-- Staging model: Clean and standardize raw contract data
{{ config(
    materialized='view',
    indexes=[
        {'columns': ['contrato_id'], 'unique': true},
        {'columns': ['data_assinatura']},
        {'columns': ['orgao_cnpj']}
    ]
) }}

SELECT
    -- Business keys and identifiers
    numeroControlePncpCompra AS contrato_id,

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

    -- Flattened JSON fields (assuming Python script flattens them)
    tipoContrato_nome,
    tipoContrato_id,
    orgaoEntidade_cnpj AS orgao_cnpj,
    orgaoEntidade_razaoSocial AS orgao_razao_social,
    orgaoEntidade_poderId AS orgao_poder_id,
    orgaoEntidade_esferaId AS orgao_esfera_id,
    categoriaProcesso_nome,
    categoriaProcesso_id,
    unidadeOrgao_ufNome AS uf_nome,
    unidadeOrgao_ufSigla AS uf_sigla,
    unidadeOrgao_municipioNome AS municipio_nome,
    unidadeOrgao_codigoIbge AS municipio_codigo_ibge,
    unidadeOrgao_nomeUnidade AS unidade_nome,
    unidadeOrgao_codigoUnidade AS unidade_codigo,

    -- Calculated fields
    CASE
        WHEN TRY_CAST(dataVigenciaFim AS DATE) IS NOT NULL
             AND TRY_CAST(dataVigenciaInicio AS DATE) IS NOT NULL
        THEN DATE_DIFF('day',
                      TRY_CAST(dataVigenciaInicio AS DATE),
                      TRY_CAST(dataVigenciaFim AS DATE))
        ELSE NULL
    END AS duracao_contrato_dias

FROM {{ source('pncp_raw_data', 'contratos_publicacao') }}
