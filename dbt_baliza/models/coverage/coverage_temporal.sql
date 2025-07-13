-- Temporal coverage analysis: tracks data availability since PNCP beginning
{{ config(
    materialized='table',
    indexes=[
        {'columns': ['data_referencia']},
        {'columns': ['uf_sigla']},
        {'columns': ['mes_ano']}
    ]
) }}

WITH 

-- PNCP started collecting data in January 2021
pncp_timeline AS (
    SELECT 
        GENERATE_SERIES(
            DATE '2021-01-01',  -- PNCP official start
            CURRENT_DATE,
            INTERVAL '1 day'
        )::DATE AS data_referencia
),

-- Add temporal dimensions
timeline_enriched AS (
    SELECT 
        data_referencia,
        EXTRACT(YEAR FROM data_referencia) AS ano,
        EXTRACT(MONTH FROM data_referencia) AS mes,
        EXTRACT(DOW FROM data_referencia) AS dia_semana,  -- 0=Sunday, 1=Monday, etc
        DATE_TRUNC('month', data_referencia)::DATE AS mes_inicio,
        CONCAT(EXTRACT(YEAR FROM data_referencia), '-', 
               LPAD(EXTRACT(MONTH FROM data_referencia)::VARCHAR, 2, '0')) AS mes_ano,
        
        -- Business days (excluding weekends)
        CASE 
            WHEN EXTRACT(DOW FROM data_referencia) IN (0, 6) THEN FALSE  -- Weekend
            ELSE TRUE  -- Business day
        END AS dia_util,
        
        -- Expected publication days (typically business days, excluding holidays)
        CASE 
            WHEN EXTRACT(DOW FROM data_referencia) IN (0, 6) THEN FALSE  -- Weekend
            WHEN data_referencia IN (
                DATE '2021-01-01', DATE '2021-04-21', DATE '2021-09-07', DATE '2021-10-12', 
                DATE '2021-11-02', DATE '2021-11-15', DATE '2021-12-25',  -- 2021 holidays sample
                DATE '2022-01-01', DATE '2022-04-15', DATE '2022-09-07', DATE '2022-10-12',
                DATE '2022-11-02', DATE '2022-11-15', DATE '2022-12-25',  -- 2022 holidays sample
                DATE '2023-01-01', DATE '2023-04-07', DATE '2023-09-07', DATE '2023-10-12',
                DATE '2023-11-02', DATE '2023-11-15', DATE '2023-12-25',  -- 2023 holidays sample
                DATE '2024-01-01', DATE '2024-03-29', DATE '2024-09-07', DATE '2024-10-12',
                DATE '2024-11-02', DATE '2024-11-15', DATE '2024-12-25',  -- 2024 holidays sample
                DATE '2025-01-01'  -- 2025 New Year
            ) THEN FALSE  -- National holidays
            ELSE TRUE  -- Expected publication day
        END AS dia_publicacao_esperado
),

-- Aggregate Baliza data by date and state
baliza_daily AS (
    SELECT 
        baliza_data_date AS data_referencia,
        uf_sigla,
        COUNT(DISTINCT contrato_id) AS contratos_coletados,
        COUNT(DISTINCT orgao_cnpj) AS orgaos_ativos,
        SUM(valor_global_brl) / 1000000 AS valor_total_milhoes,
        MIN(baliza_extracted_at) AS primeira_coleta,
        MAX(baliza_extracted_at) AS ultima_coleta
    FROM {{ ref('stg_contratos') }}
    WHERE baliza_data_date >= DATE '2021-01-01'
      AND NOT flag_valor_invalido
    GROUP BY 1, 2
),

-- Coverage analysis by date and state
coverage_by_date_uf AS (
    SELECT 
        t.data_referencia,
        t.ano,
        t.mes,
        t.mes_ano,
        t.dia_util,
        t.dia_publicacao_esperado,
        
        -- State-level analysis (we'll add a summary row for Brazil)
        COALESCE(b.uf_sigla, 'TOTAL') AS uf_sigla,
        
        -- Data availability flags
        CASE WHEN b.contratos_coletados > 0 THEN TRUE ELSE FALSE END AS tem_dados_baliza,
        COALESCE(b.contratos_coletados, 0) AS contratos_coletados,
        COALESCE(b.orgaos_ativos, 0) AS orgaos_ativos,
        COALESCE(b.valor_total_milhoes, 0) AS valor_total_milhoes,
        
        -- Coverage metrics
        CASE 
            WHEN NOT t.dia_publicacao_esperado THEN 'NAO_ESPERADO'  -- Weekend/holiday
            WHEN b.contratos_coletados > 0 THEN 'COM_DADOS'
            WHEN t.data_referencia > CURRENT_DATE - INTERVAL '7 days' THEN 'RECENTE_SEM_DADOS'
            ELSE 'SEM_DADOS'
        END AS status_cobertura,
        
        -- Time since PNCP start
        DATE_DIFF('day', DATE '2021-01-01', t.data_referencia) AS dias_desde_inicio_pncp
        
    FROM timeline_enriched t
    LEFT JOIN baliza_daily b ON t.data_referencia = b.data_referencia
    WHERE t.data_referencia <= CURRENT_DATE  -- Don't include future dates
),

-- Add rolling averages and trends
coverage_with_trends AS (
    SELECT 
        *,
        
        -- 7-day rolling average of contracts
        AVG(contratos_coletados) OVER (
            PARTITION BY uf_sigla 
            ORDER BY data_referencia 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS contratos_media_7d,
        
        -- 30-day rolling average
        AVG(contratos_coletados) OVER (
            PARTITION BY uf_sigla 
            ORDER BY data_referencia 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS contratos_media_30d,
        
        -- Days since last data
        CASE 
            WHEN tem_dados_baliza THEN 0
            ELSE ROW_NUMBER() OVER (
                PARTITION BY uf_sigla, 
                           SUM(CASE WHEN tem_dados_baliza THEN 1 ELSE 0 END) OVER (
                               PARTITION BY uf_sigla 
                               ORDER BY data_referencia 
                               ROWS UNBOUNDED PRECEDING
                           )
                ORDER BY data_referencia
            ) - 1
        END AS dias_sem_dados,
        
        -- Coverage streak (consecutive days with data)
        ROW_NUMBER() OVER (
            PARTITION BY uf_sigla,
                       SUM(CASE WHEN NOT tem_dados_baliza THEN 1 ELSE 0 END) OVER (
                           PARTITION BY uf_sigla 
                           ORDER BY data_referencia 
                           ROWS UNBOUNDED PRECEDING
                       )
            ORDER BY data_referencia
        ) - 1 AS sequencia_dias_com_dados
        
    FROM coverage_by_date_uf
)

-- Final output with additional analysis
SELECT 
    data_referencia,
    ano,
    mes,
    mes_ano,
    uf_sigla,
    dia_util,
    dia_publicacao_esperado,
    
    -- Data availability
    tem_dados_baliza,
    status_cobertura,
    contratos_coletados,
    orgaos_ativos,
    valor_total_milhoes,
    
    -- Trends and patterns
    ROUND(contratos_media_7d, 2) AS contratos_media_7d,
    ROUND(contratos_media_30d, 2) AS contratos_media_30d,
    dias_sem_dados,
    sequencia_dias_com_dados,
    
    -- Data gaps analysis
    CASE 
        WHEN dias_sem_dados = 0 THEN 'ATUAL'
        WHEN dias_sem_dados <= 7 THEN 'GAP_RECENTE'
        WHEN dias_sem_dados <= 30 THEN 'GAP_MEDIO'
        ELSE 'GAP_LONGO'
    END AS classificacao_gap,
    
    -- Historical context
    dias_desde_inicio_pncp,
    ROUND(dias_desde_inicio_pncp / 365.25, 1) AS anos_desde_inicio_pncp,
    
    -- Coverage quality indicators
    CASE 
        WHEN NOT dia_publicacao_esperado THEN NULL  -- Don't count weekends/holidays
        WHEN contratos_coletados >= contratos_media_30d * 0.5 THEN 'BOA'
        WHEN contratos_coletados > 0 THEN 'PARCIAL'  
        ELSE 'SEM_DADOS'
    END AS qualidade_cobertura

FROM coverage_with_trends
ORDER BY uf_sigla, data_referencia