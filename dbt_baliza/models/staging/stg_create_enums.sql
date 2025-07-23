{{
  config(
    materialized='ephemeral'
  )
}}

{% set enums = [
    'benefit_type', 'bidding_mode', 'call_instrument', 'contract_term_type',
    'contract_type', 'item_situation', 'judgment_criteria', 'modalidade_contratacao',
    'natureza_juridica', 'process_category', 'result_situation',
    'situacao_contratacao', 'uf_brasil'
] %}

{% for enum_name in enums %}
  {{ create_enum_from_seed(enum_name) }}
{% endfor %}

-- This model does not produce any data, it only creates types.
-- Return a dummy query to satisfy dbt.
SELECT 1 AS dummy_column
