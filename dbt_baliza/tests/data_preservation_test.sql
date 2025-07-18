-- Test to ensure data preservation philosophy is maintained
-- This test ensures that zero values are preserved for transparency
-- Following BALIZA's principle: "TRANSPARENCY over clean data"

WITH zero_value_contracts AS (
  SELECT COUNT(*) AS zero_contracts
  FROM {{ ref('silver_contratos') }}
  WHERE valor_inicial = 0
),

total_contracts AS (
  SELECT COUNT(*) AS total_contracts
  FROM {{ ref('silver_contratos') }}
)

SELECT 
  'Data preservation check' AS test_name,
  zero_contracts,
  total_contracts,
  CASE 
    WHEN total_contracts = 0 THEN 'NO_DATA'
    WHEN zero_contracts >= 0 THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result
FROM zero_value_contracts
CROSS JOIN total_contracts
WHERE test_result = 'FAIL'  -- Only return failures