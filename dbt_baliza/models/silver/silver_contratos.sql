{{
  config(
    materialized='incremental',
    unique_key='numeroControlePNCPContrato'
  )
}}

SELECT
  "numeroControlePNCPContrato",
  "numeroControlePNCP",
  "dataAssinatura"::DATE AS "dataAssinatura",
  "dataVigenciaInicio"::DATE AS "dataVigenciaInicio",
  "dataVigenciaFim"::DATE AS "dataVigenciaFim",
  "valorTotalEstimado"::DECIMAL(15, 4) AS "valorTotalEstimado"
FROM {{ ref('bronze_pncp_raw') }}
WHERE "numeroControlePNCPContrato" IS NOT NULL

{% if is_incremental() %}
AND "dataAtualizacaoPNCP" > (SELECT max("dataAtualizacaoPNCP") FROM {{ this }})
{% endif %}
