{{
  config(
    materialized='table',
    description='Fact table for procurements (contratações)',
    indexes=[
      {'columns': ['numero_controle_pncp'], 'unique': True},
      {'columns': ['data_publicacao_pncp']},
      {'columns': ['ano_compra']},
      {'columns': ['modalidade_id']},
      {'columns': ['org_key']},
      {'columns': ['unit_key']}
    ]
  )
}}

WITH procurements_with_keys AS (
  SELECT
    p.*,
    
    -- Organization keys
    org.org_key,
    unit.unit_key,
    
    -- Subrogated organization keys
    subrog_org.org_key AS subrog_org_key,
    subrog_unit.unit_key AS subrog_unit_key
    
  FROM {{ ref('gld_contratacoes') }} p
  
  -- Main organization
  LEFT JOIN {{ ref('dim_organizacoes') }} org
    ON p.orgao_entidade_json ->> 'cnpj' = org.cnpj
  
  -- Main unit
  LEFT JOIN {{ ref('dim_unidades_orgao') }} unit
    ON p.orgao_entidade_json ->> 'cnpj' = unit.cnpj_orgao
    AND p.unidade_orgao_json ->> 'codigoUnidade' = unit.codigo_unidade
  
  -- Subrogated organization
  LEFT JOIN {{ ref('dim_organizacoes') }} subrog_org
    ON p.orgao_subrogado_json ->> 'cnpj' = subrog_org.cnpj
  
  -- Subrogated unit
  LEFT JOIN {{ ref('dim_unidades_orgao') }} subrog_unit
    ON p.orgao_subrogado_json ->> 'cnpj' = subrog_unit.cnpj_orgao
    AND p.unidade_subrogada_json ->> 'codigoUnidade' = subrog_unit.codigo_unidade
)

SELECT
  -- Surrogate key
  MD5(numero_controle_pncp) AS procurement_key,
  
  -- Natural key
  numero_controle_pncp,
  
  -- Procurement identifiers
  numero_compra,
  ano_compra,
  sequencial_compra,
  
  -- Dates
  data_publicacao_pncp,
  data_abertura_proposta,
  data_encerramento_proposta,
  data_inclusao,
  data_atualizacao,
  data_atualizacao_global,
  
  -- Duration calculations
  CASE 
    WHEN data_abertura_proposta IS NOT NULL AND data_encerramento_proposta IS NOT NULL
    THEN DATE_DIFF('day', data_abertura_proposta, data_encerramento_proposta)
    ELSE NULL
  END AS duracao_proposta_dias,
  
  -- Amounts
  valor_total_estimado,
  valor_total_homologado,
  
  -- Procurement details
  objeto_compra,
  informacao_complementar,
  processo,
  link_sistema_origem,
  link_processo_eletronico,
  justificativa_presencial,
  
  -- Procurement method and mode
  modalidade_id,
  modalidade_nome,
  modalidade, -- from API parameter
  modo_disputa_id,
  modo_disputa_nome,
  
  -- Instrument and framework
  tipo_instrumento_convocatorio_codigo,
  tipo_instrumento_convocatorio_nome,
  
  -- Status and flags
  situacao_compra_id,
  situacao_compra_nome,
  srp,
  existe_resultado,
  
  -- User information
  usuario_nome,
  
  -- Foreign keys
  org_key,
  unit_key,
  subrog_org_key,
  subrog_unit_key,
  
  -- Legal framework information (extracted from JSON)
  amparo_legal_json ->> 'codigo' AS amparo_legal_codigo,
  amparo_legal_json ->> 'nome' AS amparo_legal_nome,
  amparo_legal_json ->> 'descricao' AS amparo_legal_descricao,
  
  -- Derived attributes
  CASE 
    WHEN modalidade_id = 1 THEN 'Leilão Eletrônico'
    WHEN modalidade_id = 3 THEN 'Concurso'
    WHEN modalidade_id = 4 THEN 'Concorrência Eletrônica'
    WHEN modalidade_id = 6 THEN 'Pregão Eletrônico'
    WHEN modalidade_id = 8 THEN 'Dispensa'
    WHEN modalidade_id = 9 THEN 'Inexigibilidade'
    WHEN modalidade_id = 10 THEN 'Credenciamento'
    WHEN modalidade_id = 11 THEN 'Seleção'
    WHEN modalidade_id = 12 THEN 'Consulta'
    WHEN modalidade_id = 13 THEN 'Registro de Preço'
    WHEN modalidade_id = 14 THEN 'Outros'
    ELSE 'Não informado'
  END AS modalidade_descricao,
  
  CASE 
    WHEN valor_total_estimado IS NOT NULL AND valor_total_estimado > 0 THEN
      CASE 
        WHEN valor_total_estimado <= 17600 THEN 'Até R$ 17.600'
        WHEN valor_total_estimado <= 88000 THEN 'R$ 17.601 a R$ 88.000'
        WHEN valor_total_estimado <= 176000 THEN 'R$ 88.001 a R$ 176.000'
        WHEN valor_total_estimado <= 1408000 THEN 'R$ 176.001 a R$ 1.408.000'
        WHEN valor_total_estimado <= 3300000 THEN 'R$ 1.408.001 a R$ 3.300.000'
        ELSE 'Acima de R$ 3.300.000'
      END
    ELSE 'Não informado'
  END AS faixa_valor_estimado,
  
  CASE 
    WHEN situacao_compra_id = '1' THEN 'Planejada'
    WHEN situacao_compra_id = '2' THEN 'Publicada'
    WHEN situacao_compra_id = '3' THEN 'Homologada'
    WHEN situacao_compra_id = '4' THEN 'Deserta/Fracassada'
    ELSE 'Não informado'
  END AS situacao_compra_descricao,
  
  CASE 
    WHEN data_abertura_proposta IS NOT NULL AND data_encerramento_proposta IS NOT NULL THEN
      CASE 
        WHEN DATE_DIFF('day', data_abertura_proposta, data_encerramento_proposta) <= 7 THEN 'Até 7 dias'
        WHEN DATE_DIFF('day', data_abertura_proposta, data_encerramento_proposta) <= 15 THEN '8 a 15 dias'
        WHEN DATE_DIFF('day', data_abertura_proposta, data_encerramento_proposta) <= 30 THEN '16 a 30 dias'
        WHEN DATE_DIFF('day', data_abertura_proposta, data_encerramento_proposta) <= 60 THEN '31 a 60 dias'
        ELSE 'Mais de 60 dias'
      END
    ELSE 'Não informado'
  END AS faixa_duracao_proposta,
  
  -- Data quality flags
  CASE 
    WHEN numero_controle_pncp IS NULL THEN 'Número de controle ausente'
    WHEN modalidade_id IS NULL THEN 'Modalidade ausente'
    WHEN valor_total_estimado IS NULL OR valor_total_estimado <= 0 THEN 'Valor estimado inválido'
    WHEN data_publicacao_pncp IS NULL THEN 'Data de publicação ausente'
    WHEN objeto_compra IS NULL THEN 'Objeto da compra ausente'
    ELSE 'OK'
  END AS quality_flag,
  
  -- Metadata
  endpoint_name,
  data_date,
  extracted_at,
  run_id,
  
  -- JSON fallback
  procurement_json,
  fontes_orcamentarias_json,
  
  -- Audit
  CURRENT_TIMESTAMP AS created_at,
  CURRENT_TIMESTAMP AS updated_at

FROM procurements_with_keys
ORDER BY numero_controle_pncp