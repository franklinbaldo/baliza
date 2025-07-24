import ibis
from ibis import _

def transform_silver_contratacoes(con: ibis.BaseBackend) -> ibis.Table:
    """
    Transforms raw PNCP data into the silver_contratacoes table.
    """
    bronze_pncp_raw = con.table("bronze_pncp_raw")

    silver_contratacoes = bronze_pncp_raw.mutate(
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

    return silver_contratacoes

def transform_silver_dim_unidades_orgao(con: ibis.BaseBackend) -> ibis.Table:
    """
    Creates a dimension table for organizational units.
    """
    bronze_pncp_raw = con.table("bronze_pncp_raw")

    silver_dim_unidades_orgao = bronze_pncp_raw[
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

    return silver_dim_unidades_orgao


def create_gold_contratacoes_analytics(
    silver_contratacoes: ibis.Table, silver_dim_unidades_orgao: ibis.Table
) -> ibis.Table:
    """
    Creates an analytical view of contratacoes.
    """
    # Use modalidade_nome if available (enriched), otherwise modalidadeId
    modalidade_column = (
        silver_contratacoes.modalidade_nome 
        if "modalidade_nome" in silver_contratacoes.columns
        else silver_contratacoes.modalidadeId
    )
    
    gold_analytics = (
        silver_contratacoes.join(
            silver_dim_unidades_orgao,
            silver_contratacoes.sequencialOrgao == silver_dim_unidades_orgao.sequencialOrgao,
            how="left",
        )
        .group_by(
            [
                silver_contratacoes.anoContratacao,
                modalidade_column.name("modalidadeNome"),
                silver_dim_unidades_orgao.nomeOrgao,
            ]
        )
        .aggregate(
            quantidade_contratacoes=silver_contratacoes.numeroControlePNCP.count(),
            valor_total_estimado=silver_contratacoes.valorTotalEstimado.sum(),
        )
    )

    return gold_analytics
