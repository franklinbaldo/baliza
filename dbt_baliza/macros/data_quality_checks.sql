-- DBT macros for data quality checks

{% macro check_data_freshness(table_name, date_column, max_age_hours=25) %}
  SELECT 
    '{{ table_name }}' AS table_name,
    'data_freshness' AS check_name,
    CASE 
      WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX({{ date_column }}))) / 3600 <= {{ max_age_hours }}
      THEN 'PASS'
      ELSE 'FAIL'
    END AS check_status,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX({{ date_column }}))) / 3600 AS hours_since_last_data,
    {{ max_age_hours }} AS threshold_hours
  FROM {{ table_name }}
{% endmacro %}

{% macro check_duplicate_contracts() %}
  SELECT 
    'psa.contratos_raw' AS table_name,
    'duplicate_contracts' AS check_name,
    CASE 
      WHEN COUNT(*) = 0 THEN 'PASS'
      ELSE 'FAIL'
    END AS check_status,
    COUNT(*) AS duplicate_count,
    0 AS threshold
  FROM (
    SELECT numeroControlePncpCompra
    FROM {{ source('psa', 'contratos_raw') }}
    WHERE numeroControlePncpCompra IS NOT NULL
    GROUP BY numeroControlePncpCompra
    HAVING COUNT(*) > 1
  ) duplicates
{% endmacro %}

{% macro check_value_distribution() %}
  SELECT 
    'contratos' AS table_name,
    'value_distribution' AS check_name,
    CASE 
      WHEN q99_value / NULLIF(median_value, 0) < 1000 THEN 'PASS'
      WHEN q99_value / NULLIF(median_value, 0) < 10000 THEN 'WARNING'
      ELSE 'FAIL'
    END AS check_status,
    q99_value / NULLIF(median_value, 0) AS ratio_q99_median,
    1000 AS threshold
  FROM (
    SELECT 
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY valor_global_brl) AS median_value,
      PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY valor_global_brl) AS q99_value
    FROM {{ ref('stg_contratos') }}
    WHERE valor_global_brl > 0
  ) stats
{% endmacro %}

{% macro generate_data_quality_report() %}
  WITH quality_checks AS (
    {{ check_data_freshness('{{ source("psa", "contratos_raw") }}', 'baliza_extracted_at') }}
    
    UNION ALL
    
    {{ check_duplicate_contracts() }}
    
    UNION ALL
    
    {{ check_value_distribution() }}
  )
  
  SELECT 
    CURRENT_TIMESTAMP AS report_timestamp,
    table_name,
    check_name,
    check_status,
    CASE 
      WHEN check_name = 'data_freshness' THEN hours_since_last_data
      WHEN check_name = 'duplicate_contracts' THEN duplicate_count
      WHEN check_name = 'value_distribution' THEN ratio_q99_median
    END AS check_value,
    threshold AS check_threshold
  FROM quality_checks
{% endmacro %}