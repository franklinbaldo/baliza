{{
  config(
    materialized='table'
  )
}}

WITH orgaos_unicos AS (
    SELECT DISTINCT
        "sequencialOrgao",
        "cnpjOrgao",
        "siglaOrgao",
        "nomeOrgao",
        "codigoEsfera",
        "nomeEsfera",
        "codigoPoder",
        "nomePoder",
        "uf"
    FROM
        {{ ref('bronze_pncp_raw') }}
)
SELECT
    "sequencialOrgao",
    "cnpjOrgao",
    "siglaOrgao",
    "nomeOrgao",
    "codigoEsfera",
    "nomeEsfera",
    "codigoPoder",
    "nomePoder",
    "uf"
FROM
    orgaos_unicos
