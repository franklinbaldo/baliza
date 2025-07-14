

WITH contracts_with_keys AS (
  SELECT
    c.*,
    
    -- Organization keys
    org.org_key,
    unit.unit_key,
    
    -- Subrogated organization keys
    subrog_org.org_key AS subrog_org_key,
    subrog_unit.unit_key AS subrog_unit_key
    
  FROM "baliza"."main_staging"."stg_contratos_raw" c
  
  -- Main organization
  LEFT JOIN "baliza"."main_dimensions"."dim_organizacoes" org
    ON c.orgao_entidade_json ->> 'cnpj' = org.cnpj
  
  -- Main unit
  LEFT JOIN "baliza"."main_dimensions"."dim_unidades_orgao" unit
    ON c.orgao_entidade_json ->> 'cnpj' = unit.cnpj_orgao
    AND CAST(c.unidade_orgao_json ->> 'codigoUnidade' AS VARCHAR) = CAST(unit.codigo_unidade AS VARCHAR)
  
  -- Subrogated organization
  LEFT JOIN "baliza"."main_dimensions"."dim_organizacoes" subrog_org
    ON c.orgao_subrogado_json ->> 'cnpj' = subrog_org.cnpj
  
  -- Subrogated unit
  LEFT JOIN "baliza"."main_dimensions"."dim_unidades_orgao" subrog_unit
    ON c.orgao_subrogado_json ->> 'cnpj' = subrog_unit.cnpj_orgao
    AND CAST(c.unidade_subrogada_json ->> 'codigoUnidade' AS VARCHAR) = CAST(subrog_unit.codigo_unidade AS VARCHAR)
)

SELECT
  -- Surrogate key
  MD5(numero_controle_pncp) AS contract_key,
  
  -- Natural key
  numero_controle_pncp,
  
  -- Contract identifiers
  numero_controle_pncp_compra,
  numero_contrato_empenho,
  ano_contrato,
  sequencial_contrato,
  
  -- Dates
  data_assinatura,
  data_vigencia_inicio,
  data_vigencia_fim,
  data_publicacao_pncp,
  data_atualizacao,
  data_atualizacao_global,
  
  -- Duration calculations
  CASE 
    WHEN data_vigencia_inicio IS NOT NULL AND data_vigencia_fim IS NOT NULL
    THEN data_vigencia_fim - data_vigencia_inicio
    ELSE NULL
  END AS duracao_vigencia_dias,
  
  -- Amounts
  valor_inicial,
  valor_global,
  valor_parcela,
  valor_acumulado,
  
  -- Supplier information
  ni_fornecedor,
  tipo_pessoa,
  nome_razao_social_fornecedor,
  ni_fornecedor_subcontratado,
  nome_fornecedor_subcontratado,
  tipo_pessoa_subcontratada,
  
  -- Contract details
  objeto_contrato,
  informacao_complementar,
  processo,
  numero_parcelas,
  numero_retificacao,
  receita,
  
  -- Additional identifiers
  codigo_pais_fornecedor,
  identificador_cipi,
  url_cipi,
  usuario_nome,
  
  -- Foreign keys
  org_key,
  unit_key,
  subrog_org_key,
  subrog_unit_key,
  
  -- Type information (extracted from JSON)
  tipo_contrato_json ->> 'id' AS tipo_contrato_id,
  tipo_contrato_json ->> 'nome' AS tipo_contrato_nome,
  categoria_processo_json ->> 'id' AS categoria_processo_id,
  categoria_processo_json ->> 'nome' AS categoria_processo_nome,
  
  -- Derived attributes
  CASE 
    WHEN tipo_pessoa = 'PJ' THEN 'Pessoa Jurídica'
    WHEN tipo_pessoa = 'PF' THEN 'Pessoa Física'
    WHEN tipo_pessoa = 'PE' THEN 'Pessoa Estrangeira'
    ELSE 'Outros'
  END AS tipo_pessoa_descricao,
  
  CASE 
    WHEN valor_global IS NOT NULL AND valor_global > 0 THEN
      CASE 
        WHEN valor_global <= 17600 THEN 'Até R$ 17.600'
        WHEN valor_global <= 88000 THEN 'R$ 17.601 a R$ 88.000'
        WHEN valor_global <= 176000 THEN 'R$ 88.001 a R$ 176.000'
        WHEN valor_global <= 1408000 THEN 'R$ 176.001 a R$ 1.408.000'
        WHEN valor_global <= 3300000 THEN 'R$ 1.408.001 a R$ 3.300.000'
        ELSE 'Acima de R$ 3.300.000'
      END
    ELSE 'Não informado'
  END AS faixa_valor_global,
  
  CASE 
    WHEN data_vigencia_inicio IS NOT NULL AND data_vigencia_fim IS NOT NULL THEN
      CASE 
        WHEN data_vigencia_fim - data_vigencia_inicio <= 90 THEN 'Até 90 dias'
        WHEN data_vigencia_fim - data_vigencia_inicio <= 365 THEN '91 a 365 dias'
        WHEN data_vigencia_fim - data_vigencia_inicio <= 730 THEN '1 a 2 anos'
        WHEN data_vigencia_fim - data_vigencia_inicio <= 1825 THEN '2 a 5 anos'
        ELSE 'Mais de 5 anos'
      END
    ELSE 'Não informado'
  END AS faixa_duracao_vigencia,
  
  -- Data quality flags
  CASE 
    WHEN numero_controle_pncp IS NULL THEN 'Número de controle ausente'
    WHEN valor_global IS NULL OR valor_global <= 0 THEN 'Valor global inválido'
    WHEN data_assinatura IS NULL THEN 'Data de assinatura ausente'
    WHEN ni_fornecedor IS NULL THEN 'NI do fornecedor ausente'
    ELSE 'OK'
  END AS quality_flag,
  
  -- Metadata
  endpoint_name,
  data_date,
  extracted_at,
  run_id,
  
  -- JSON fallback
  contract_json,
  
  -- Audit
  CURRENT_TIMESTAMP AS created_at,
  CURRENT_TIMESTAMP AS updated_at

FROM contracts_with_keys
ORDER BY numero_controle_pncp