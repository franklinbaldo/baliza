-- Coverage analysis by endpoint and year
-- Tracks data coverage by API endpoint and year, comparing local data with Internet Archive availability
{{ config(
    materialized='table',
    indexes=[
        {'columns': ['endpoint']},
        {'columns': ['ano']},
        {'columns': ['endpoint', 'ano']}
    ]
) }}

WITH 

-- Get all available endpoints and their data years
endpoint_years AS (
    SELECT 
        'contratos' AS endpoint,
        EXTRACT(YEAR FROM data_assinatura) AS ano,
        COUNT(*) AS total_registros,
        COUNT(DISTINCT orgao_cnpj) AS total_orgaos,
        SUM(CASE WHEN data_source = 'internet_archive' THEN 1 ELSE 0 END) AS registros_ia,
        SUM(CASE WHEN data_source = 'internet_archive' THEN 1.0 ELSE 0 END) / COUNT(*) * 100 AS percentual_ia,
        MIN(data_assinatura) AS data_primeiro_registro,
        MAX(data_assinatura) AS data_ultimo_registro
    FROM {{ ref('stg_contratos') }}
    WHERE data_assinatura IS NOT NULL
    GROUP BY 1, 2
    
    UNION ALL
    
    -- Add other endpoints as needed following the same pattern
    -- Example for a different endpoint:
    -- SELECT 
    --     'licitacoes' AS endpoint,
    --     EXTRACT(YEAR FROM data_abertura) AS ano,
    --     COUNT(*) AS total_registros,
    --     -- ... other metrics
    -- FROM {{ ref('stg_licitacoes') }}
    -- WHERE data_abertura IS NOT NULL
    -- GROUP BY 1, 2
    
    -- Add more endpoints here as they become available
),

-- Get download history by endpoint and year
download_history AS (
    SELECT 
        'contratos' AS endpoint,
        EXTRACT(YEAR FROM baliza_data_date) AS ano,
        COUNT(DISTINCT DATE(baliza_data_date)) AS dias_download,
        MIN(baliza_data_date) AS primeiro_download,
        MAX(baliza_data_date) AS ultimo_download,
        COUNT(DISTINCT baliza_run_id) AS total_execucoes
    FROM {{ ref('stg_contratos') }}
    WHERE baliza_data_date IS NOT NULL
    GROUP BY 1, 2
    
    -- Add other endpoints as needed following the same pattern
)

-- Final coverage metrics by endpoint and year
SELECT 
    ey.endpoint,
    ey.ano,
    ey.total_registros,
    ey.total_orgaos,
    ey.registros_ia,
    ey.percentual_ia,
    ey.data_primeiro_registro,
    ey.data_ultimo_registro,
    
    -- Download metrics
    COALESCE(dh.dias_download, 0) AS dias_download,
    dh.primeiro_download,
    dh.ultimo_download,
    COALESCE(dh.total_execucoes, 0) AS total_execucoes,
    
    -- Coverage metrics
    CASE 
        WHEN ey.total_registros > 0 
        THEN ROUND(ey.registros_ia * 100.0 / ey.total_registros, 2) 
        ELSE 0 
    END AS cobertura_ia_percentual,
    
    -- Data freshness
    CURRENT_DATE - DATE(ey.data_ultimo_registro) AS dias_desde_ultimo_registro,
    
    -- Metadata
    CURRENT_TIMESTAMP AS data_atualizacao
    
FROM endpoint_years ey
LEFT JOIN download_history dh 
    ON ey.endpoint = dh.endpoint 
    AND ey.ano = dh.ano

UNION ALL

-- Add placeholder rows for years with downloads but no data
-- This helps track when we tried to download data but found nothing
SELECT 
    dh.endpoint,
    dh.ano,
    0 AS total_registros,
    0 AS total_orgaos,
    0 AS registros_ia,
    0 AS percentual_ia,
    NULL AS data_primeiro_registro,
    NULL AS data_ultimo_registro,
    dh.dias_download,
    dh.primeiro_download,
    dh.ultimo_download,
    dh.total_execucoes,
    0 AS cobertura_ia_percentual,
    NULL AS dias_desde_ultimo_registro,
    CURRENT_TIMESTAMP AS data_atualizacao
FROM download_history dh
WHERE NOT EXISTS (
    SELECT 1 
    FROM endpoint_years ey 
    WHERE ey.endpoint = dh.endpoint 
    AND ey.ano = dh.ano
)

ORDER BY endpoint, ano DESC
