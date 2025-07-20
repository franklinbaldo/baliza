-- Macro for data quality monitoring that preserves data but provides transparency
-- Following BALIZA's data preservation philosophy: monitor anomalies, don't filter them

{% macro monitor_data_quality(model_ref, column_name, anomaly_condition, description) %}
  {% set view_name = model_ref.name ~ '_' ~ column_name ~ '_quality_monitor' %}
  CREATE OR REPLACE VIEW {{ view_name }} AS
  SELECT
    '{{ column_name }}' AS monitored_column,
    '{{ description }}' AS anomaly_description,
    COUNT(*) AS anomaly_count,
    (SELECT COUNT(*) FROM {{ model_ref }}) AS total_records,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {{ model_ref }}) AS anomaly_percentage,
    MIN({{ column_name }}) AS min_value,
    MAX({{ column_name }}) AS max_value,
    CURRENT_TIMESTAMP AS monitored_at
  FROM {{ model_ref }}
  WHERE {{ anomaly_condition }}
{% endmacro %}

{% macro create_transparency_report(model_ref, columns_to_profile) %}
  {% set view_name = model_ref.name ~ '_transparency_report' %}
  CREATE OR REPLACE VIEW {{ view_name }} AS
  SELECT
    '{{ model_ref.name }}' AS table_name,
    COUNT(*) AS total_records,
    
    {% for column in columns_to_profile %}
      {{ profile_column(column) }}
    {% endfor %}
    
    CURRENT_TIMESTAMP AS report_generated_at
  FROM {{ model_ref }}
{% endmacro %}

{% macro profile_column(column_name) %}
  COUNT(DISTINCT {{ column_name }}) AS unique_{{ column_name }},
  COUNT(CASE WHEN {{ column_name }} IS NULL THEN 1 END) AS null_{{ column_name }},
{% endmacro %}

{% macro monitor_orphaned_records(from_model, from_column, to_model, to_column) %}
  {% set view_name = from_model.name ~ '_' ~ from_column ~ '_orphaned_monitor' %}
  CREATE OR REPLACE VIEW {{ view_name }} AS
  SELECT
    '{{ from_column }}' AS foreign_key_column,
    '{{ to_model.name }}.{{ to_column }}' AS referenced_column,
    COUNT(*) AS orphaned_records,
    (SELECT COUNT(*) FROM {{ from_model }}) AS total_records,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {{ from_model }}) AS orphaned_percentage
  FROM {{ from_model }} AS source
  LEFT JOIN {{ to_model }} AS target
    ON source.{{ from_column }} = target.{{ to_column }}
  WHERE target.{{ to_column }} IS NULL
    AND source.{{ from_column }} IS NOT NULL
{% endmacro %}