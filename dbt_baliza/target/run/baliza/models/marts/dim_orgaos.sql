
  
    
    

    create  table
      "baliza_dbt"."main_marts"."dim_orgaos__dbt_tmp"
  
    as (
      -- Dimension table: Government entities (órgãos)


WITH orgaos_agg AS (
    SELECT 
        orgao_cnpj,
        orgao_razao_social,
        orgao_poder_id,
        orgao_esfera_id,
        uf_sigla,
        uf_nome,
        
        -- Aggregated metrics
        COUNT(DISTINCT contrato_id) AS total_contratos,
        SUM(valor_global_brl) AS valor_total_contratado,
        AVG(valor_global_brl) AS valor_medio_contrato,
        MIN(data_assinatura) AS primeiro_contrato_data,
        MAX(data_assinatura) AS ultimo_contrato_data,
        
        -- Data quality metrics
        SUM(CASE WHEN flag_valor_suspeito THEN 1 ELSE 0 END) AS contratos_valor_suspeito,
        SUM(CASE WHEN flag_duracao_suspeita THEN 1 ELSE 0 END) AS contratos_duracao_suspeita,
        
        -- Most recent data
        MAX(baliza_extracted_at) AS ultima_atualizacao
        
    FROM "baliza_dbt"."main_staging"."stg_contratos"
    WHERE orgao_cnpj IS NOT NULL
    GROUP BY 1,2,3,4,5,6
),

orgaos_enriched AS (
    SELECT 
        *,
        
        -- Classifications
        CASE orgao_poder_id
            WHEN 'E' THEN 'Executivo'
            WHEN 'L' THEN 'Legislativo'
            WHEN 'J' THEN 'Judiciário'
            WHEN 'M' THEN 'Ministério Público'
            ELSE 'Outros'
        END AS poder_nome,
        
        CASE orgao_esfera_id
            WHEN 'F' THEN 'Federal'
            WHEN 'E' THEN 'Estadual'
            WHEN 'M' THEN 'Municipal'
            ELSE 'Outros'
        END AS esfera_nome,
        
        -- Risk scoring
        CASE 
            WHEN (contratos_valor_suspeito::FLOAT / NULLIF(total_contratos, 0)) > 0.1 THEN 'Alto'
            WHEN (contratos_valor_suspeito::FLOAT / NULLIF(total_contratos, 0)) > 0.05 THEN 'Médio'
            ELSE 'Baixo'
        END AS risco_valor,
        
        -- Performance metrics
        valor_total_contratado / NULLIF(total_contratos, 0) AS valor_medio_verificado,
        DATE_DIFF('day', primeiro_contrato_data, ultimo_contrato_data) AS periodo_atividade_dias
        
    FROM orgaos_agg
)

SELECT * FROM orgaos_enriched
    );
  
  