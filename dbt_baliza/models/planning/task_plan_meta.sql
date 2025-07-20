{{
  config(
    materialized=var('planning_materialization', 'table'),
    unique_key='plan_fingerprint'
  )
}}

-- Plan generation metadata for versioning and drift detection
-- ADR-009: dbt-Driven Task Planning Architecture

WITH plan_metadata AS (
  SELECT
    '{{ var("plan_fingerprint") }}' AS plan_fingerprint,
    '{{ var("plan_environment") }}' AS environment,
    '{{ var("start_date") }}'::date AS date_range_start,
    '{{ var("end_date") }}'::date AS date_range_end,
    '{{ run_started_at }}'::timestamp AS generated_at,
    '{{ var("config_version") }}' AS config_version
),

task_count AS (
  SELECT
    plan_fingerprint,
    COUNT(*) AS task_count
  FROM {{ ref('task_plan') }}
  GROUP BY plan_fingerprint
)

SELECT 
  pm.plan_fingerprint,
  pm.environment,
  pm.date_range_start,
  pm.date_range_end,
  pm.generated_at,
  pm.config_version,
  tc.task_count
FROM plan_metadata pm
JOIN task_count tc ON pm.plan_fingerprint = tc.plan_fingerprint