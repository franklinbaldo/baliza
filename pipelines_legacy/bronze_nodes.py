import ibis
from ibis import _

def create_bronze_content_analysis(pncp_content: ibis.Table, pncp_requests: ibis.Table) -> ibis.Table:
    content_metrics = pncp_content.join(
        pncp_requests, pncp_content.id == pncp_requests.content_id
    ).select(
        pncp_content.id.name("content_id"),
        pncp_content.reference_count,
        pncp_content.content_size_bytes,
        pncp_requests.endpoint_name,
        pncp_requests.data_date,
    )

    analysis = content_metrics.group_by(["endpoint_name", "data_date"]).agg(
        unique_content_records=_.content_id.count(),
        total_references=_.reference_count.sum(),
        actual_storage_bytes=_.content_size_bytes.sum(),
        theoretical_storage_bytes=(_.content_size_bytes * _.reference_count).sum(),
        deduplicated_content_count=(
            _.reference_count > 1
        ).ifelse(1, 0).sum(),
    )

    analysis = analysis.mutate(
        deduplication_rate_percent=(
            analysis.deduplicated_content_count / analysis.unique_content_records
        )
        * 100,
        storage_savings_bytes=(
            analysis.theoretical_storage_bytes - analysis.actual_storage_bytes
        ),
    )

    analysis = analysis.mutate(
        storage_savings_percent=(
            analysis.storage_savings_bytes / analysis.theoretical_storage_bytes
        )
        * 100,
        compression_ratio=(
            analysis.actual_storage_bytes / analysis.theoretical_storage_bytes
        ),
        analysis_timestamp=ibis.now(),
    )

    return analysis


def create_bronze_pncp_raw(pncp_content: ibis.Table) -> ibis.Table:
    return pncp_content.select(
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
        "fundamentacaoLegal",
    )


def create_bronze_pncp_requests(pncp_requests: ibis.Table) -> ibis.Table:
    return pncp_requests.select("url_path", "response_time", "extracted_at")
