{{
  config(
    materialized='view',
    description='Bronze layer for PNCP raw responses'
  )
}}

SELECT
    id,
    extracted_at,
    endpoint_name,
    endpoint_url,
    data_date,
    run_id,
    modalidade,
    total_records,
    total_pages,
    current_page,
    response_code,
    response_content
FROM {{ source('psa', 'pncp_raw_responses') }}
