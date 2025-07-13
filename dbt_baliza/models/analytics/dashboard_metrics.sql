-- Basic KPIs for monitoring data collection (Core Baliza functionality only)
{{ config(
    materialized='table',
    indexes=[
        {'columns': ['data_referencia']},
        {'columns': ['uf_sigla']}
    ]
) }}

SELECT 
    baliza_data_date AS data_referencia,
    uf_sigla,
    
    -- Basic volume metrics
    COUNT(DISTINCT contrato_id) AS total_contratos,
    COUNT(DISTINCT orgao_cnpj) AS total_orgaos,
    COUNT(DISTINCT fornecedor_ni) AS total_fornecedores,
    
    -- Basic financial metrics (in millions BRL)
    ROUND(SUM(valor_global_brl) / 1000000, 2) AS valor_total_milhoes_brl,
    ROUND(AVG(valor_global_brl) / 1000000, 4) AS valor_medio_milhoes_brl,
    
    -- Data quality metrics
    COUNT(*) AS total_registros,
    SUM(CASE WHEN valor_global_brl > 0 THEN 1 ELSE 0 END) AS registros_com_valor,
    SUM(CASE WHEN data_assinatura IS NOT NULL THEN 1 ELSE 0 END) AS registros_com_data
    
FROM {{ ref('stg_contratos') }}
WHERE baliza_data_date >= CURRENT_DATE - INTERVAL '30 days'  -- Last 30 days
GROUP BY 1, 2
ORDER BY data_referencia DESC, valor_total_milhoes_brl DESC