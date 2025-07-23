{{
  config(
    materialized='table',
    unique_key='claim_id',
    indexes=[
      {'columns': ['task_id'], 'type': 'btree'},
      {'columns': ['status'], 'type': 'btree'},
      {'columns': ['expires_at'], 'type': 'btree'},
      {'columns': ['worker_id'], 'type': 'btree'}
    ]
  )
}}

-- Task claiming table for atomic execution coordination
-- ADR-009: dbt-Driven Task Planning Architecture

-- This table is populated by Python code during execution
-- dbt models only read from it for analytics and monitoring

SELECT
  claim_id,
  task_id,
  claimed_at,
  expires_at,
  worker_id,
  status
FROM {{ source('runtime', 'task_claims') }}

-- Note: This model is just for schema definition and analytics
-- Actual claims are inserted by Python extractor using atomic
-- SELECT...FOR UPDATE operations for concurrency safety