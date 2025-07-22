{% macro enum_drift_detection(column_name, seed_name) %}
  WITH all_values AS (
    SELECT DISTINCT {{ column_name }} AS value FROM {{ ref('bronze_pncp_raw') }}
  ),
  seed_values AS (
    SELECT code AS value FROM {{ ref(seed_name) }}
  )
  SELECT
    av.value
  FROM all_values av
  LEFT JOIN seed_values sv ON av.value = sv.value
  WHERE sv.value IS NULL
{% endmacro %}
