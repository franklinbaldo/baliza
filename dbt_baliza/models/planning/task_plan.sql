{{
  config(
    materialized=var('planning_materialization', 'table'),
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
    start_date=var('start_date'),
    end_date=var('end_date')
) }}