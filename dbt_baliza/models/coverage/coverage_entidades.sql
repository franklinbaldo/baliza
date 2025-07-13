-- Entity coverage analysis: which public entities are publishing contracts
{{ config(
    materialized='table',
    indexes=[
        {'columns': ['cnpj']},
        {'columns': ['uf']},
        {'columns': ['status_publicacao']}
    ]
) }}

WITH

-- Reference entities from seed
ref_entities AS (
    SELECT 
        cnpj,
        razao_social,
        uf,
        found_in_baliza,
        total_contratos,
        TRY_CAST(primeiro_contrato AS DATE) AS primeiro_contrato,
        TRY_CAST(ultimo_contrato AS DATE) AS ultimo_contrato
    FROM {{ ref('ref_entidades_publicas') }}
),

-- Actual contract data from Baliza
baliza_entities AS (
    SELECT 
        orgao_cnpj AS cnpj,
        orgao_razao_social,
        uf_sigla,
        COUNT(DISTINCT contrato_id) AS contratos_baliza,
        COUNT(DISTINCT fornecedor_ni) AS fornecedores_unicos,
        SUM(valor_global_brl) / 1000000 AS valor_total_milhoes,
        MIN(data_assinatura) AS primeiro_contrato_baliza,
        MAX(data_assinatura) AS ultimo_contrato_baliza,
        MIN(baliza_data_date) AS primeira_coleta,
        MAX(baliza_data_date) AS ultima_coleta,
        
        -- Activity patterns
        COUNT(DISTINCT DATE_TRUNC('month', data_assinatura)) AS meses_ativos,
        COUNT(DISTINCT EXTRACT(YEAR FROM data_assinatura)) AS anos_ativos,
        
        -- Recent activity (last 90 days)
        SUM(CASE 
            WHEN data_assinatura >= CURRENT_DATE - INTERVAL '90 days' 
            THEN 1 ELSE 0 
        END) AS contratos_ultimos_90d,
        
        -- Temporal distribution
        AVG(valor_global_brl) / 1000000 AS valor_medio_milhoes,
        STDDEV(valor_global_brl) / 1000000 AS valor_desvio_milhoes
        
    FROM {{ ref('stg_contratos') }}
    WHERE orgao_cnpj IS NOT NULL 
      AND NOT flag_valor_invalido
    GROUP BY 1, 2, 3
),

-- Expected entities (from CNPJ database) with coverage analysis
coverage_analysis AS (
    SELECT 
        r.cnpj,
        r.razao_social AS razao_social_cnpj,
        r.uf,
        
        -- Baliza data (if available)
        b.orgao_razao_social AS razao_social_baliza,
        b.uf_sigla,
        COALESCE(b.contratos_baliza, 0) AS contratos_baliza,
        COALESCE(b.fornecedores_unicos, 0) AS fornecedores_unicos,
        COALESCE(b.valor_total_milhoes, 0) AS valor_total_milhoes,
        COALESCE(b.valor_medio_milhoes, 0) AS valor_medio_milhoes,
        
        -- Temporal coverage
        b.primeiro_contrato_baliza,
        b.ultimo_contrato_baliza,
        b.primeira_coleta,
        b.ultima_coleta,
        COALESCE(b.meses_ativos, 0) AS meses_ativos,
        COALESCE(b.anos_ativos, 0) AS anos_ativos,
        COALESCE(b.contratos_ultimos_90d, 0) AS contratos_ultimos_90d,
        
        -- Data quality indicators
        CASE 
            WHEN r.razao_social IS NOT NULL AND b.orgao_razao_social IS NOT NULL
                 AND UPPER(r.razao_social) != UPPER(b.orgao_razao_social) 
            THEN TRUE ELSE FALSE 
        END AS divergencia_nome,
        
        CASE 
            WHEN r.uf IS NOT NULL AND b.uf_sigla IS NOT NULL
                 AND r.uf != b.uf_sigla 
            THEN TRUE ELSE FALSE 
        END AS divergencia_uf,
        
        -- Coverage status
        CASE 
            WHEN b.cnpj IS NOT NULL AND b.contratos_ultimos_90d > 0 THEN 'ATIVO_RECENTE'
            WHEN b.cnpj IS NOT NULL AND b.ultimo_contrato_baliza >= CURRENT_DATE - INTERVAL '365 days' THEN 'ATIVO_ULTIMO_ANO'
            WHEN b.cnpj IS NOT NULL THEN 'INATIVO'
            WHEN r.cnpj IS NOT NULL THEN 'NAO_ENCONTRADO'
            ELSE 'DESCONHECIDO'
        END AS status_publicacao,
        
        -- Days since last activity
        CASE 
            WHEN b.ultimo_contrato_baliza IS NOT NULL 
            THEN DATE_DIFF('day', b.ultimo_contrato_baliza, CURRENT_DATE)
            ELSE NULL 
        END AS dias_sem_atividade,
        
        -- Coverage score (0-100)
        CASE 
            WHEN b.cnpj IS NULL THEN 0  -- Not found
            WHEN b.contratos_ultimos_90d > 0 THEN 100  -- Active recently
            WHEN b.ultimo_contrato_baliza >= CURRENT_DATE - INTERVAL '180 days' THEN 75  -- Active last 6 months
            WHEN b.ultimo_contrato_baliza >= CURRENT_DATE - INTERVAL '365 days' THEN 50  -- Active last year
            WHEN b.ultimo_contrato_baliza >= CURRENT_DATE - INTERVAL '730 days' THEN 25  -- Active last 2 years
            ELSE 10  -- Found but very old data
        END AS score_cobertura
        
    FROM ref_entities r
    FULL OUTER JOIN baliza_entities b ON r.cnpj = b.cnpj
),

-- Add comparative analysis
coverage_with_benchmarks AS (
    SELECT 
        *,
        
        -- State-level benchmarks
        AVG(contratos_baliza) OVER (PARTITION BY uf) AS contratos_media_uf,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY contratos_baliza) OVER (PARTITION BY uf) AS contratos_mediana_uf,
        
        -- Activity classification relative to state
        CASE 
            WHEN contratos_baliza = 0 THEN 'INATIVO'
            WHEN contratos_baliza >= PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY contratos_baliza) OVER (PARTITION BY uf) THEN 'MUITO_ATIVO'
            WHEN contratos_baliza >= PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY contratos_baliza) OVER (PARTITION BY uf) THEN 'ATIVO'
            WHEN contratos_baliza >= PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY contratos_baliza) OVER (PARTITION BY uf) THEN 'MODERADO'
            ELSE 'BAIXO'
        END AS classificacao_atividade,
        
        -- Data completeness indicators
        CASE 
            WHEN primeiro_contrato_baliza IS NOT NULL 
                 AND primeiro_contrato_baliza <= DATE '2021-06-01'  -- Early PNCP adopter
            THEN TRUE ELSE FALSE 
        END AS adocao_precoce_pncp,
        
        -- Consistency indicators
        CASE 
            WHEN ultimo_contrato_baliza IS NOT NULL 
                 AND ultima_coleta IS NOT NULL
                 AND DATE_DIFF('day', ultimo_contrato_baliza, ultima_coleta) <= 7
            THEN TRUE ELSE FALSE 
        END AS dados_atualizados
        
    FROM coverage_analysis
)

-- Final output with summary metrics
SELECT 
    cnpj,
    COALESCE(razao_social_baliza, razao_social_cnpj) AS razao_social,
    COALESCE(uf_sigla, uf) AS uf,
    status_publicacao,
    score_cobertura,
    classificacao_atividade,
    
    -- Contract metrics
    contratos_baliza,
    fornecedores_unicos,
    valor_total_milhoes,
    valor_medio_milhoes,
    
    -- Temporal metrics
    primeiro_contrato_baliza,
    ultimo_contrato_baliza,
    meses_ativos,
    anos_ativos,
    contratos_ultimos_90d,
    dias_sem_atividade,
    
    -- Data quality
    divergencia_nome,
    divergencia_uf,
    adocao_precoce_pncp,
    dados_atualizados,
    
    -- Benchmarks
    ROUND(contratos_media_uf, 1) AS contratos_media_uf,
    contratos_mediana_uf,
    
    -- Collection metadata
    primeira_coleta,
    ultima_coleta,
    
    -- Coverage insights
    CASE 
        WHEN status_publicacao = 'NAO_ENCONTRADO' THEN 'Entidade pública não encontrada no PNCP'
        WHEN status_publicacao = 'INATIVO' THEN 'Sem contratos recentes (>1 ano)'
        WHEN status_publicacao = 'ATIVO_ULTIMO_ANO' THEN 'Ativo no último ano'
        WHEN status_publicacao = 'ATIVO_RECENTE' THEN 'Ativo nos últimos 90 dias'
        ELSE 'Status desconhecido'
    END AS observacao_cobertura,
    
    -- Update timestamp
    CURRENT_TIMESTAMP AS atualizado_em
    
FROM coverage_with_benchmarks
ORDER BY uf, score_cobertura DESC, contratos_baliza DESC