{% macro extract_json_data(json_field, fields_to_extract) %}
  {% for field, alias, type in fields_to_extract %}
    {% if type %}
      TRY_CAST({{ json_field }} ->> '{{ field }}' AS {{ type }}) AS {{ alias }},
    {% else %}
      {{ json_field }} ->> '{{ field }}' AS {{ alias }},
    {% endif %}
  {% endfor %}
{% endmacro %}