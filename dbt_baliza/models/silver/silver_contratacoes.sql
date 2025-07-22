{{
  config(
    materialized='incremental',
    unique_key='numeroControlePNCP'
  )
}}

SELECT
  "numeroControlePNCP",
  "anoContratacao",
  "dataPublicacaoPNCP"::DATE AS "dataPublicacaoPNCP",
  "dataAtualizacaoPNCP"::DATE AS "dataAtualizacaoPNCP",
  "horaAtualizacaoPNCP",
  "sequencialOrgao",
  "cnpjOrgao"::VARCHAR(14) AS "cnpjOrgao",
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
  "uf"::uf_brasil_enum AS "uf",
  "amparoLegalId",
  "amparoLegalNome",
  "modalidadeId"::modalidade_contratacao_enum AS "modalidadeId",
  "modalidadeNome",
  "numeroContratacao",
  "processo",
  "objetoContratacao",
  "codigoSituacaoContratacao"::situacao_contratacao_enum AS "codigoSituacaoContratacao",
  "nomeSituacaoContratacao",
  "valorTotalEstimado"::DECIMAL(15, 4) AS "valorTotalEstimado",
  "informacaoComplementar",
  "dataAssinatura"::DATE AS "dataAssinatura",
  "dataVigenciaInicio"::DATE AS "dataVigenciaInicio",
  "dataVigenciaFim"::DATE AS "dataVigenciaFim",
  "numeroControlePNCPContrato",
  "justificativa",
  "fundamentacaoLegal"
FROM {{ ref('bronze_pncp_raw') }}

{% if is_incremental() %}
WHERE "dataAtualizacaoPNCP" > (SELECT max("dataAtualizacaoPNCP") FROM {{ this }})
{% endif %}
