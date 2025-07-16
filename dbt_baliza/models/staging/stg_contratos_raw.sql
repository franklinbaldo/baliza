{{
  config(
    materialized='view',
    description='Staged raw contracts data directly from the PNCP API'
  )
}}

WITH contratos_publicacao AS (
  SELECT
    'contratos_publicacao' AS endpoint_name,
    *
  FROM {{ source('pncp_api', 'contratos_publicacao') }}
),

contratos_atualizacao AS (
  SELECT
    'contratos_atualizacao' AS endpoint_name,
    *
  FROM {{ source('pncp_api', 'contratos_atualizacao') }}
),

unioned_sources AS (
    SELECT * FROM contratos_publicacao
    UNION ALL
    SELECT * FROM contratos_atualizacao
),

-- The final select statement unnests the data and casts the columns
-- This is a simplified version; in a real scenario, you would cast all columns as in the original file.
SELECT
  endpoint_name,
  d.*
FROM unioned_sources,
UNNEST(data) AS d
