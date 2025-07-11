-- Analytics model: Dashboard metrics and KPIs


WITH daily_metrics AS (
    SELECT 
        baliza_data_date AS data_referencia,
        uf_sigla,
        categoria_processo_nome,
        orgao_poder_id,
        orgao_esfera_id,
        
        -- Volume metrics
        COUNT(DISTINCT contrato_id) AS total_contratos,
        COUNT(DISTINCT orgao_cnpj) AS total_orgaos,
        COUNT(DISTINCT fornecedor_ni) AS total_fornecedores,
        
        -- Financial metrics (in millions BRL)
        ROUND(SUM(valor_global_brl) / 1000000, 2) AS valor_total_milhoes_brl,
        ROUND(AVG(valor_global_brl) / 1000000, 4) AS valor_medio_milhoes_brl,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY valor_global_brl) / 1000000, 4) AS valor_mediano_milhoes_brl,
        ROUND(MAX(valor_global_brl) / 1000000, 2) AS valor_maximo_milhoes_brl,
        
        -- Duration metrics
        AVG(duracao_contrato_dias) AS duracao_media_dias,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duracao_contrato_dias) AS duracao_mediana_dias,
        MAX(duracao_contrato_dias) AS duracao_maxima_dias,
        
        -- Risk metrics
        SUM(CASE WHEN flag_valor_suspeito THEN 1 ELSE 0 END) AS contratos_valor_suspeito,
        SUM(CASE WHEN flag_duracao_suspeita THEN 1 ELSE 0 END) AS contratos_duracao_suspeita,
        
        -- Percentages
        ROUND(
            100.0 * SUM(CASE WHEN flag_valor_suspeito THEN 1 ELSE 0 END) / COUNT(*), 
            2
        ) AS perc_valor_suspeito,
        
        -- Geographic distribution
        COUNT(DISTINCT municipio_codigo_ibge) AS total_municipios,
        
        -- Temporal patterns
        SUM(CASE WHEN EXTRACT(DOW FROM data_assinatura) IN (0, 6) THEN 1 ELSE 0 END) AS contratos_fim_semana,
        
        -- Emergency contracts (keywords in object)
        SUM(CASE 
            WHEN LOWER(objeto_contrato) LIKE '%emergenc%' 
                OR LOWER(objeto_contrato) LIKE '%urgente%' 
                OR LOWER(objeto_contrato) LIKE '%urgênci%'
            THEN 1 ELSE 0 
        END) AS contratos_emergencia
        
    FROM "baliza_dbt"."main_staging"."stg_contratos"
    WHERE baliza_data_date >= CURRENT_DATE - INTERVAL '30 days'  -- Last 30 days
    GROUP BY 1,2,3,4,5
),

enriched_metrics AS (
    SELECT 
        *,
        
        -- Growth rates (vs previous day)
        LAG(total_contratos) OVER (
            PARTITION BY uf_sigla, categoria_processo_nome 
            ORDER BY data_referencia
        ) AS contratos_dia_anterior,
        
        LAG(valor_total_milhoes_brl) OVER (
            PARTITION BY uf_sigla, categoria_processo_nome 
            ORDER BY data_referencia
        ) AS valor_dia_anterior,
        
        -- Moving averages (7-day)
        AVG(total_contratos) OVER (
            PARTITION BY uf_sigla, categoria_processo_nome 
            ORDER BY data_referencia 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS contratos_media_7d,
        
        AVG(valor_total_milhoes_brl) OVER (
            PARTITION BY uf_sigla, categoria_processo_nome 
            ORDER BY data_referencia 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS valor_media_7d_milhoes,
        
        -- Rankings
        ROW_NUMBER() OVER (
            PARTITION BY data_referencia 
            ORDER BY valor_total_milhoes_brl DESC
        ) AS rank_valor_diario,
        
        ROW_NUMBER() OVER (
            PARTITION BY data_referencia 
            ORDER BY total_contratos DESC
        ) AS rank_volume_diario
        
    FROM daily_metrics
),

final_metrics AS (
    SELECT 
        *,
        
        -- Growth calculations
        CASE 
            WHEN contratos_dia_anterior > 0 THEN 
                ROUND(100.0 * (total_contratos - contratos_dia_anterior) / contratos_dia_anterior, 2)
            ELSE NULL 
        END AS crescimento_contratos_perc,
        
        CASE 
            WHEN valor_dia_anterior > 0 THEN 
                ROUND(100.0 * (valor_total_milhoes_brl - valor_dia_anterior) / valor_dia_anterior, 2)
            ELSE NULL 
        END AS crescimento_valor_perc,
        
        -- Anomaly detection (simple z-score based)
        CASE 
            WHEN ABS(total_contratos - contratos_media_7d) > (2 * STDDEV(total_contratos) OVER (
                PARTITION BY uf_sigla, categoria_processo_nome 
                ORDER BY data_referencia 
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            )) THEN TRUE 
            ELSE FALSE 
        END AS anomalia_volume,
        
        CASE 
            WHEN ABS(valor_total_milhoes_brl - valor_media_7d_milhoes) > (2 * STDDEV(valor_total_milhoes_brl) OVER (
                PARTITION BY uf_sigla, categoria_processo_nome 
                ORDER BY data_referencia 
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            )) THEN TRUE 
            ELSE FALSE 
        END AS anomalia_valor
        
    FROM enriched_metrics
)

SELECT * FROM final_metrics
ORDER BY data_referencia DESC, valor_total_milhoes_brl DESC