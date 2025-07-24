import ibis
from ibis import _

def transform_stage_contratacoes(con: ibis.BaseBackend) -> ibis.Table:
    """
    Transforms raw PNCP data into the stage_contratacoes table.
    """
    raw_pncp_data = con.table("raw_pncp_data")

    stage_contratacoes = raw_pncp_data.mutate(
        dataPublicacaoPNCP=_.dataPublicacaoPNCP.cast("date"),
        dataAtualizacaoPNCP=_.dataAtualizacaoPNCP.cast("date"),
        cnpjOrgao=_.cnpjOrgao.cast("string"),
        uf=_.uf.cast("string"),  # Assuming uf_brasil_enum is a string type in ibis
        modalidadeId=_.modalidadeId.cast("string"),  # Assuming modalidade_contratacao_enum is a string type
        codigoSituacaoContratacao=_.codigoSituacaoContratacao.cast("string"),  # Assuming situacao_contratacao_enum is a string
        valorTotalEstimado=_.valorTotalEstimado.cast("decimal(15, 4)"),
        dataAssinatura=_.dataAssinatura.cast("date"),
        dataVigenciaInicio=_.dataVigenciaInicio.cast("date"),
        dataVigenciaFim=_.dataVigenciaFim.cast("date"),
    )

    return stage_contratacoes

def transform_stage_dim_unidades_orgao(con: ibis.BaseBackend) -> ibis.Table:
    """
    Creates a dimension table for organizational units.
    """
    raw_pncp_data = con.table("raw_pncp_data")

    stage_dim_unidades_orgao = raw_pncp_data[
        "sequencialOrgao",
        "cnpjOrgao",
        "siglaOrgao",
        "nomeOrgao",
        "codigoEsfera",
        "nomeEsfera",
        "codigoPoder",
        "nomePoder",
        "uf",
    ].distinct()

    return stage_dim_unidades_orgao


def create_mart_contratacoes_analytics(
    stage_contratacoes: ibis.Table, stage_dim_unidades_orgao: ibis.Table
) -> ibis.Table:
    """
    Creates an analytical mart view of contratacoes.
    """
    # Use modalidade_nome if available (enriched), otherwise modalidadeId
    modalidade_column = (
        stage_contratacoes.modalidade_nome 
        if "modalidade_nome" in stage_contratacoes.columns
        else stage_contratacoes.modalidadeId
    )
    
    mart_analytics = (
        stage_contratacoes.join(
            stage_dim_unidades_orgao,
            stage_contratacoes.sequencialOrgao == stage_dim_unidades_orgao.sequencialOrgao,
            how="left",
        )
        .group_by(
            [
                stage_contratacoes.anoContratacao,
                modalidade_column.name("modalidadeNome"),
                stage_dim_unidades_orgao.nomeOrgao,
            ]
        )
        .aggregate(
            quantidade_contratacoes=stage_contratacoes.numeroControlePNCP.count(),
            valor_total_estimado=stage_contratacoes.valorTotalEstimado.sum(),
        )
    )

    return mart_analytics
