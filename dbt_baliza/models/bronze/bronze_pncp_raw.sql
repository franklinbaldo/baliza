{{
  config(
    materialized='incremental',
    unique_key='numeroControlePNCP'
  )
}}

SELECT
  "numeroControlePNCP",
  "anoContratacao",
  "dataPublicacaoPNCP",
  "dataAtualizacaoPNCP",
  "horaAtualizacaoPNCP",
  "sequencialOrgao",
  "cnpjOrgao",
  "siglaOrgao",
  "nomeOrgao",
  "sequencialUnidade",
  "codigoUnidade",
  "siglaUnidade",
  "nomeUnidade",
  "codigoEsfera",
  "nomeEsfera",
  "codigoPoder",
  "nomePoder",
  "codigoMunicipio",
  "nomeMunicipio",
  "uf",
  "amparoLegalId",
  "amparoLegalNome",
  "modalidadeId",
  "modalidadeNome",
  "numeroContratacao",
  "processo",
  "objetoContratacao",
  "codigoSituacaoContratacao",
  "nomeSituacaoContratacao",
  "valorTotalEstimado",
  "informacaoComplementar",
  "dataAssinatura",
  "dataVigenciaInicio",
  "dataVigenciaFim",
  "numeroControlePNCPContrato",
  "justificativa",
  "fundamentacaoLegal"
FROM {{ source('bronze', 'pncp_content') }}

{% if is_incremental() %}
WHERE "dataAtualizacaoPNCP" > (SELECT max("dataAtualizacaoPNCP") FROM {{ this }})
{% endif %}
