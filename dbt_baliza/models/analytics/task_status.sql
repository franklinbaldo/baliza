{{
  config(
    materialized='view'
  )
}}

-- Real-time task status aggregation for operational monitoring
-- ADR-009: dbt-Driven Task Planning Architecture

WITH current_claims AS (
  -- Get current active claims per task
  SELECT 
    task_id,
    MAX(claimed_at) AS latest_claim_at,
    COUNT(*) AS claim_count
  FROM {{ ref('task_claims') }}
  WHERE status IN ('CLAIMED', 'EXECUTING')
    AND expires_at > CURRENT_TIMESTAMP
  GROUP BY task_id
),

task_completion AS (
  -- Determine completion status from results
  SELECT 
    tr.task_id,
    COUNT(*) AS completed_tasks,
    MAX(tr.completed_at) AS last_completed_at,
    SUM(tr.records_extracted) AS total_records
  FROM {{ source('runtime', 'task_results') }} tr
  WHERE tr.status = 'SUCCESS'
  GROUP BY tr.task_id
),

task_status_summary AS (
  SELECT 
    tp.task_id,
    tp.endpoint_name,
    tp.data_date,
    tp.modalidade,
    tp.plan_fingerprint,
    tp.created_at AS task_created_at,
    
    -- Determine current status
    CASE 
      WHEN tc.completed_tasks > 0 THEN 'COMPLETED'
      WHEN cc.task_id IS NOT NULL THEN 'CLAIMED'
      ELSE 'PENDING'
    END AS current_status,
    
    -- Metrics
    COALESCE(tc.completed_tasks, 0) AS completed_tasks,
    COALESCE(tc.total_records, 0) AS total_records,
    cc.claim_count,
    tc.last_completed_at,
    cc.latest_claim_at,
    
    -- Calculate duration if completed
    CASE 
      WHEN tc.last_completed_at IS NOT NULL 
      THEN EXTRACT(EPOCH FROM (tc.last_completed_at - tp.created_at)) / 3600.0
      ELSE NULL 
    END AS completion_hours

  FROM {{ ref('task_plan') }} tp
  LEFT JOIN current_claims cc ON tp.task_id = cc.task_id
  LEFT JOIN task_completion tc ON tp.task_id = tc.task_id
)

SELECT 
  task_id,
  endpoint_name,
  data_date,
  modalidade,
  current_status,
  completed_tasks,
  total_records,
  completion_hours,
  task_created_at,
  last_completed_at,
  latest_claim_at,
  plan_fingerprint
FROM task_status_summary
ORDER BY endpoint_name, data_date, modalidade