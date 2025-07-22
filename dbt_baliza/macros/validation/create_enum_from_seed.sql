{% macro create_enum_from_seed(seed_name) %}
  {% set query %}
    SELECT array_agg(code) FROM {{ ref(seed_name) }}
  {% endset %}

  {% set results = run_query(query) %}

  {% if execute %}
    {% set enum_values = results.columns[0].values() %}

    CREATE TYPE IF NOT EXISTS {{ seed_name }}_enum AS ENUM (
      {% for value in enum_values %}
        '{{ value }}'
        {% if not loop.last %},{% endif %}
      {% endfor %}
    );
  {% endif %}
{% endmacro %}
