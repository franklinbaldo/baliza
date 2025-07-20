{{
  config(
    materialized='table',
    unique_key='task_id',
    indexes=[
      {'columns': ['endpoint_name', 'data_date'], 'type': 'btree'},
      {'columns': ['status'], 'type': 'btree'},
      {'columns': ['plan_fingerprint'], 'type': 'btree'}
    ]
  )
}}

-- Generate extraction task plan from unified configuration
-- ADR-009: dbt-Driven Task Planning Architecture

{{ generate_task_plan(
    start_date=var('plan_start_date', '2023-01-01'),
    end_date=var('plan_end_date', '2024-12-31'),
    environment=var('plan_environment', 'prod')
) }}