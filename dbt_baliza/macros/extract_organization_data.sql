{% macro extract_organization_data(json_field, prefix) %}
  {{ json_field }} ->> 'cnpj' AS {{ prefix }}_cnpj,
  {{ json_field }} ->> 'razaoSocial' AS {{ prefix }}_razao_social,
  {{ json_field }} ->> 'poderId' AS {{ prefix }}_poder_id,
  {{ json_field }} ->> 'esferaId' AS {{ prefix }}_esfera_id
{% endmacro %}

{% macro extract_unit_data(json_field, prefix) %}
  {{ json_field }} ->> 'ufNome' AS {{ prefix }}_uf_nome,
  {{ json_field }} ->> 'ufSigla' AS {{ prefix }}_uf_sigla,
  {{ json_field }} ->> 'codigoUnidade' AS {{ prefix }}_codigo_unidade,
  {{ json_field }} ->> 'nomeUnidade' AS {{ prefix }}_nome_unidade,
  {{ json_field }} ->> 'municipioNome' AS {{ prefix }}_municipio_nome,
  {{ json_field }} ->> 'codigoIbge' AS {{ prefix }}_codigo_ibge
{% endmacro %}

{% macro extract_legal_framework_data(json_field, prefix) %}
  CAST({{ json_field }} ->> 'codigo' AS INTEGER) AS {{ prefix }}_codigo,
  {{ json_field }} ->> 'nome' AS {{ prefix }}_nome,
  {{ json_field }} ->> 'descricao' AS {{ prefix }}_descricao
{% endmacro %}

{% macro extract_type_data(json_field, prefix) %}
  CAST({{ json_field }} ->> 'id' AS INTEGER) AS {{ prefix }}_id,
  {{ json_field }} ->> 'nome' AS {{ prefix }}_nome
{% endmacro %}