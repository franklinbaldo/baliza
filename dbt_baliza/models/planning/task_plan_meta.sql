{{
  config(
    materialized='table',
    unique_key='plan_version'
  )
}}

-- Plan generation metadata for versioning and drift detection
-- ADR-009: dbt-Driven Task Planning Architecture

WITH plan_metadata AS (
  SELECT
    '{{ var("plan_environment", "prod") }}-{{ run_started_at.strftime("%Y-%m-%d") }}.1' AS plan_version,
    '{{ var("plan_fingerprint", "unknown") }}' AS plan_fingerprint,
    '{{ var("plan_environment", "prod") }}' AS environment,
    '{{ var("plan_start_date", "2023-01-01") }}'::date AS date_range_start,
    '{{ var("plan_end_date", "2024-12-31") }}'::date AS date_range_end,
    '{{ run_started_at }}'::timestamp AS generated_at,
    '{{ var("config_version", "1.0") }}' AS config_version
),

task_count AS (
  -- Count tasks in current plan
  SELECT COUNT(*) AS task_count
  FROM {{ ref('task_plan') }}
  WHERE plan_fingerprint = '{{ var("plan_fingerprint", "unknown") }}'
)

SELECT 
  pm.plan_version,
  pm.plan_fingerprint,
  pm.environment,
  pm.date_range_start,
  pm.date_range_end,
  pm.generated_at,
  pm.config_version,
  tc.task_count
FROM plan_metadata pm
CROSS JOIN task_count tc