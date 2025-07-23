{% macro test_cnpj_format(model, column_name) %}
SELECT
  *
FROM
  {{ model }}
WHERE
  LENGTH({{ column_name }}) != 14
{% endmacro %}

{% macro test_cpf_format(model, column_name) %}
SELECT
  *
FROM
  {{ model }}
WHERE
  LENGTH({{ column_name }}) != 11
{% endmacro %}
