{% macro generate_task_plan(start_date, end_date, environment='prod') %}
{#
  Generate extraction task plan from unified endpoints.yaml configuration.
  
  This macro creates a deterministic task plan that can be fingerprinted
  and compared between dbt and Python execution environments.
  
  Args:
    start_date: Start date for extraction (YYYY-MM-DD)
    end_date: End date for extraction (YYYY-MM-DD) 
    environment: Environment name (dev, staging, prod)
    
  Returns:
    SQL query that generates task_plan rows
#}

WITH endpoints_config AS (
  -- Static endpoint configuration
  -- In production, this would be loaded from endpoints.yaml via dbt-external-tables
  -- For now, we'll hardcode the active endpoints from the YAML config
  SELECT endpoint_name, modalidades, granularity, active
  FROM (VALUES
    ('contratos_publicacao', ARRAY[]::integer[], 'month', true),
    ('contratos_atualizacao', ARRAY[]::integer[], 'month', true),
    ('atas_periodo', ARRAY[]::integer[], 'month', true),
    ('atas_atualizacao', ARRAY[]::integer[], 'month', true),
    ('contratacoes_publicacao', ARRAY[1,2,3,4,5,6,7,8,9,10,11,12,99], 'month', true),
    ('contratacoes_atualizacao', ARRAY[1,2,3,4,5,6,7,8,9,10,11,12,99], 'month', true),
    ('pca_atualizacao', ARRAY[]::integer[], 'month', true),
    ('instrumentoscobranca_inclusao', ARRAY[]::integer[], 'month', true),
    ('contratacoes_proposta', ARRAY[]::integer[], 'month', true)
  ) AS t(endpoint_name, modalidades, granularity, active)
  WHERE active = true
),

date_spine AS (
  -- Generate date range based on granularity (DuckDB syntax)
  SELECT 
    date_trunc('month', d::TIMESTAMP) AS data_date
  FROM generate_series(
    '{{ start_date }}'::DATE,
    '{{ end_date }}'::DATE,
    INTERVAL '1 month'
  ) AS t(d)
),

endpoint_modalidade_combinations AS (
  -- Cross product of endpoints and their modalidades
  SELECT 
    e.endpoint_name,
    e.granularity,
    CASE 
      WHEN array_length(e.modalidades, 1) IS NULL THEN NULL
      ELSE unnest(e.modalidades)
    END AS modalidade
  FROM endpoints_config e
),

task_plan_raw AS (
  -- Generate task plan by crossing dates with endpoint/modalidade combinations
  SELECT 
    -- Deterministic task ID (DuckDB MD5 hash)
    md5(em.endpoint_name || '|' || ds.data_date::text || '|' || COALESCE(em.modalidade::text, 'null')) AS task_id,
    em.endpoint_name,
    ds.data_date,
    em.modalidade,
    'PENDING' AS status,
    '{{ var("plan_fingerprint", "unknown") }}' AS plan_fingerprint,
    CURRENT_TIMESTAMP AS created_at
  FROM date_spine ds
  CROSS JOIN endpoint_modalidade_combinations em
)

SELECT * FROM task_plan_raw

{% endmacro %}