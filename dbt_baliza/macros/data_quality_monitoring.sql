-- Macro for data quality monitoring that preserves data but provides transparency
-- Following BALIZA's data preservation philosophy: monitor anomalies, don't filter them

{% macro monitor_data_quality(table_name, column_name, description) %}
  -- Create a monitoring view for anomalies without filtering data
  CREATE OR REPLACE VIEW {{ table_name }}_quality_monitor AS
  SELECT
    '{{ column_name }}' AS monitored_column,
    '{{ description }}' AS anomaly_description,
    COUNT(*) AS anomaly_count,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {{ table_name }}) AS anomaly_percentage,
    MIN({{ column_name }}) AS min_value,
    MAX({{ column_name }}) AS max_value,
    CURRENT_TIMESTAMP AS monitored_at
  FROM {{ table_name }}
  WHERE {{ column_name }} IS NOT NULL
    AND (
      {{ column_name }} = 0  -- Track zero values for transparency
      OR {{ column_name }} < 0  -- Track negative values (potential data corruption)
    )
{% endmacro %}

{% macro create_transparency_report(model_name) %}
  -- Create a comprehensive data quality report for transparency
  CREATE OR REPLACE VIEW {{ model_name }}_transparency_report AS
  SELECT
    '{{ model_name }}' AS table_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT numero_controle_pncp) AS unique_contracts,
    COUNT(*) - COUNT(DISTINCT numero_controle_pncp) AS duplicate_records,

    -- Value distribution for transparency
    COUNT(CASE WHEN valor_inicial = 0 THEN 1 END) AS zero_value_contracts,
    COUNT(CASE WHEN valor_inicial < 0 THEN 1 END) AS negative_value_contracts,
    COUNT(CASE WHEN valor_inicial > 1000000 THEN 1 END) AS high_value_contracts,

    -- Date quality for transparency
    COUNT(CASE WHEN data_assinatura IS NULL THEN 1 END) AS missing_signature_dates,
    COUNT(CASE WHEN data_vigencia_fim < data_vigencia_inicio THEN 1 END) AS invalid_date_ranges,

    -- Supplier information completeness
    COUNT(CASE WHEN ni_fornecedor IS NULL THEN 1 END) AS missing_supplier_ids,
    COUNT(CASE WHEN nome_razao_social_fornecedor IS NULL THEN 1 END) AS missing_supplier_names,

    CURRENT_TIMESTAMP AS report_generated_at
  FROM {{ ref(model_name) }}
{% endmacro %}

{% macro validate_relationships_with_monitoring(from_column, to_table, to_column) %}
  -- Validate relationships but provide monitoring instead of failing
  SELECT
    '{{ from_column }}' AS foreign_key_column,
    '{{ to_table }}.{{ to_column }}' AS referenced_column,
    COUNT(*) AS orphaned_records,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {{ this }}) AS orphaned_percentage
  FROM {{ this }} AS source
  LEFT JOIN {{ ref(to_table) }} AS target
    ON source.{{ from_column }} = target.{{ to_column }}
  WHERE target.{{ to_column }} IS NULL
    AND source.{{ from_column }} IS NOT NULL
{% endmacro %}