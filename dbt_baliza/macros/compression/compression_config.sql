{% macro compression_config() %}
  {{
    config(
      materialized='table',
      options={
        'default_compression': 'zstd'
      }
    )
  }}
{% endmacro %}
