-- Depends on the ENUM types being created.
{{ ref('stg_create_enums') }}

{{
  config(
    materialized='incremental',
    unique_key='sequencialOrgao'
  )
}}

SELECT DISTINCT
  "sequencialOrgao",
  "cnpjOrgao"::VARCHAR(14) AS "cnpjOrgao",
  "siglaOrgao",
  "nomeOrgao",
  "codigoEsfera",
  "nomeEsfera",
  "codigoPoder",
  "nomePoder",
  "codigoMunicipio",
  "nomeMunicipio",
  "uf"::uf_brasil_enum AS "uf"
FROM {{ ref('bronze_pncp_raw') }}
WHERE "sequencialOrgao" IS NOT NULL

{% if is_incremental() %}
AND "dataAtualizacaoPNCP" > (SELECT max("dataAtualizacaoPNCP") FROM {{ ref('bronze_pncp_raw') }} WHERE "sequencialOrgao" IN (SELECT "sequencialOrgao" FROM {{ this }}))
{% endif %}