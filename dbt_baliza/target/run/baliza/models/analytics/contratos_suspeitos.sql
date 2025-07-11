
  
    
    

    create  table
      "baliza_dbt"."main_analytics"."contratos_suspeitos__dbt_tmp"
  
    as (
      -- Analytics model: Suspicious contracts detection


WITH contratos_scored AS (
    SELECT 
        c.*,
        o.poder_nome,
        o.esfera_nome,
        o.risco_valor AS orgao_risco_valor,
        
        -- Suspicious patterns scoring (0-100)
        (
            -- High value contracts (20 points)
            CASE WHEN c.flag_valor_suspeito THEN 20 ELSE 0 END +
            
            -- Unusual duration (15 points)
            CASE WHEN c.flag_duracao_suspeita THEN 15 ELSE 0 END +
            
            -- Same supplier frequency (15 points)
            CASE WHEN fornecedor_freq.contratos_mesmo_fornecedor > 10 THEN 15 
                 WHEN fornecedor_freq.contratos_mesmo_fornecedor > 5 THEN 10
                 ELSE 0 END +
            
            -- Round number values (10 points)
            CASE WHEN c.valor_global_brl % 10000 = 0 AND c.valor_global_brl > 100000 THEN 10 ELSE 0 END +
            
            -- Weekend signature (5 points)
            CASE WHEN EXTRACT(DOW FROM c.data_assinatura) IN (0, 6) THEN 5 ELSE 0 END +
            
            -- High-risk entity (20 points)
            CASE WHEN o.risco_valor = 'Alto' THEN 20 
                 WHEN o.risco_valor = 'Médio' THEN 10 
                 ELSE 0 END +
            
            -- Very short contracts (10 points)
            CASE WHEN c.duracao_contrato_dias <= 1 THEN 10 ELSE 0 END +
            
            -- Emergency processes (5 points - based on object keywords)
            CASE WHEN LOWER(c.objeto_contrato) LIKE '%emergenc%' 
                   OR LOWER(c.objeto_contrato) LIKE '%urgente%' 
                   OR LOWER(c.objeto_contrato) LIKE '%urgênci%'
                 THEN 5 ELSE 0 END
        ) AS score_suspeita
        
    FROM "baliza_dbt"."main_staging"."stg_contratos" c
    LEFT JOIN "baliza_dbt"."main_marts"."dim_orgaos" o 
        ON c.orgao_cnpj = o.orgao_cnpj
    LEFT JOIN (
        -- Supplier frequency analysis
        SELECT 
            fornecedor_ni,
            orgao_cnpj,
            COUNT(*) AS contratos_mesmo_fornecedor
        FROM "baliza_dbt"."main_staging"."stg_contratos"
        WHERE fornecedor_ni IS NOT NULL
        GROUP BY 1, 2
    ) fornecedor_freq 
        ON c.fornecedor_ni = fornecedor_freq.fornecedor_ni 
        AND c.orgao_cnpj = fornecedor_freq.orgao_cnpj
),

contratos_classificados AS (
    SELECT 
        *,
        
        -- Risk classification
        CASE 
            WHEN score_suspeita >= 50 THEN 'Crítico'
            WHEN score_suspeita >= 30 THEN 'Alto'
            WHEN score_suspeita >= 15 THEN 'Médio'
            ELSE 'Baixo'
        END AS nivel_risco,
        
        -- Ranking within same entity
        ROW_NUMBER() OVER (
            PARTITION BY orgao_cnpj 
            ORDER BY score_suspeita DESC, valor_global_brl DESC
        ) AS rank_suspeita_orgao
        
    FROM contratos_scored
)

SELECT * FROM contratos_classificados
WHERE score_suspeita > 0  -- Only include contracts with some suspicion
ORDER BY score_suspeita DESC, valor_global_brl DESC
    );
  
  