{{
  config(
    materialized='incremental',
    unique_key='result_id',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['task_id'], 'type': 'btree'},
      {'columns': ['completed_at'], 'type': 'btree'},
      {'columns': ['request_id'], 'type': 'btree'}
    ]
  )
}}

-- Task execution results - streaming insertions from Python
-- ADR-009: dbt-Driven Task Planning Architecture

SELECT
  result_id,
  task_id,
  request_id,
  page_number,
  records_extracted,
  completed_at
FROM {{ source('runtime', 'task_results') }}

{% if is_incremental() %}
  -- Only process new results since last run
  WHERE completed_at > (SELECT MAX(completed_at) FROM {{ this }})
{% endif %}