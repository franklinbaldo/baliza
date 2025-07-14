
  
    
    

    create  table
      "baliza"."main_marts"."mart_procurement_analytics__dbt_tmp"
  
    as (
      

WITH procurement_summary AS (
  SELECT
    p.numero_controle_pncp,
    p.ano_compra,
    p.data_publicacao_pncp,
    p.modalidade_id,
    p.modalidade_nome,
    p.modalidade_descricao,
    p.valor_total_estimado,
    p.valor_total_homologado,
    p.faixa_valor_estimado,
    p.situacao_compra_id,
    p.situacao_compra_descricao,
    p.srp,
    p.existe_resultado,
    p.objeto_compra,
    p.org_key,
    p.unit_key,
    
    -- Organization info
    org.cnpj AS org_cnpj,
    org.razao_social AS org_razao_social,
    org.poder_nome AS org_poder,
    org.esfera_nome AS org_esfera,
    
    -- Unit info
    unit.nome_unidade AS unit_nome,
    unit.uf_sigla AS unit_uf,
    unit.regiao AS unit_regiao,
    unit.municipio_nome AS unit_municipio
    
  FROM "baliza"."main_facts"."fact_contratacoes" p
  LEFT JOIN "baliza"."main_dimensions"."dim_organizacoes" org
    ON p.org_key = org.org_key
  LEFT JOIN "baliza"."main_dimensions"."dim_unidades_orgao" unit
    ON p.unit_key = unit.unit_key
),

contract_summary AS (
  SELECT
    c.numero_controle_pncp_compra,
    COUNT(*) AS total_contratos,
    SUM(c.valor_global) AS valor_total_contratos,
    MIN(c.data_assinatura) AS primeira_assinatura,
    MAX(c.data_assinatura) AS ultima_assinatura,
    AVG(c.duracao_vigencia_dias) AS duracao_media_vigencia,
    COUNT(DISTINCT c.ni_fornecedor) AS fornecedores_distintos,
    
    -- Contract status flags
    SUM(CASE WHEN c.receita = true THEN 1 ELSE 0 END) AS contratos_com_receita,
    SUM(CASE WHEN c.numero_retificacao > 0 THEN 1 ELSE 0 END) AS contratos_retificados
    
  FROM "baliza"."main_facts"."fact_contratos" c
  WHERE c.numero_controle_pncp_compra IS NOT NULL
  GROUP BY c.numero_controle_pncp_compra
)

SELECT
  -- Procurement identifiers
  p.numero_controle_pncp,
  p.ano_compra,
  p.data_publicacao_pncp,
  
  -- Organization information
  p.org_cnpj,
  p.org_razao_social,
  p.org_poder,
  p.org_esfera,
  p.unit_nome,
  p.unit_uf,
  p.unit_regiao,
  p.unit_municipio,
  
  -- Procurement details
  p.modalidade_id,
  p.modalidade_nome,
  p.modalidade_descricao,
  p.objeto_compra,
  p.situacao_compra_id,
  p.situacao_compra_descricao,
  p.srp,
  p.existe_resultado,
  
  -- Financial information
  p.valor_total_estimado,
  p.valor_total_homologado,
  p.faixa_valor_estimado,
  
  -- Contract information
  COALESCE(c.total_contratos, 0) AS total_contratos,
  COALESCE(c.valor_total_contratos, 0) AS valor_total_contratos,
  c.primeira_assinatura,
  c.ultima_assinatura,
  c.duracao_media_vigencia,
  COALESCE(c.fornecedores_distintos, 0) AS fornecedores_distintos,
  COALESCE(c.contratos_com_receita, 0) AS contratos_com_receita,
  COALESCE(c.contratos_retificados, 0) AS contratos_retificados,
  
  -- Performance metrics
  CASE 
    WHEN p.valor_total_estimado > 0 AND c.valor_total_contratos > 0 THEN
      ROUND((c.valor_total_contratos / p.valor_total_estimado) * 100, 2)
    ELSE NULL
  END AS percentual_execucao_financeira,
  
  CASE 
    WHEN p.valor_total_homologado > 0 AND p.valor_total_estimado > 0 THEN
      ROUND((p.valor_total_homologado / p.valor_total_estimado) * 100, 2)
    ELSE NULL
  END AS percentual_economia_homologacao,
  
  -- Time metrics
  CASE 
    WHEN p.data_publicacao_pncp IS NOT NULL AND c.primeira_assinatura IS NOT NULL THEN
      c.primeira_assinatura - p.data_publicacao_pncp
    ELSE NULL
  END AS dias_publicacao_primeira_assinatura,
  
  -- Categories for analysis
  CASE 
    WHEN p.modalidade_id IN (6, 4) THEN 'Competitiva'
    WHEN p.modalidade_id IN (8, 9) THEN 'Não Competitiva'
    WHEN p.modalidade_id IN (1, 3, 10, 11, 12) THEN 'Outros'
    ELSE 'Não Classificada'
  END AS categoria_modalidade,
  
  CASE 
    WHEN p.org_esfera = 'Federal' THEN 'Federal'
    WHEN p.org_esfera = 'Estadual' THEN 'Estadual'
    WHEN p.org_esfera = 'Municipal' THEN 'Municipal'
    ELSE 'Outros'
  END AS categoria_esfera,
  
  CASE 
    WHEN p.org_poder = 'Executivo' THEN 'Executivo'
    WHEN p.org_poder = 'Legislativo' THEN 'Legislativo'
    WHEN p.org_poder = 'Judiciário' THEN 'Judiciário'
    WHEN p.org_poder = 'Ministério Público' THEN 'Ministério Público'
    ELSE 'Outros'
  END AS categoria_poder,
  
  -- Quality indicators
  CASE 
    WHEN p.existe_resultado = true AND COALESCE(c.total_contratos, 0) = 0 THEN 'Resultado sem contratos'
    WHEN p.existe_resultado = false AND COALESCE(c.total_contratos, 0) > 0 THEN 'Contratos sem resultado'
    WHEN p.valor_total_estimado > 0 AND p.valor_total_homologado > p.valor_total_estimado THEN 'Homologação acima do estimado'
    ELSE 'OK'
  END AS indicador_qualidade,
  
  -- Metadata
  CURRENT_TIMESTAMP AS created_at

FROM procurement_summary p
LEFT JOIN contract_summary c
  ON p.numero_controle_pncp = c.numero_controle_pncp_compra

ORDER BY p.ano_compra DESC, p.data_publicacao_pncp DESC
    );
  
  