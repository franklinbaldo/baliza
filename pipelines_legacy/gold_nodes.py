import ibis
from ibis import _

def create_gold_contratacoes_analytics(
    silver_contratacoes: ibis.Table, silver_dim_unidades_orgao: ibis.Table
) -> ibis.Table:
    return (
        silver_contratacoes.left_join(
            silver_dim_unidades_orgao,
            silver_contratacoes.sequencialOrgao
            == silver_dim_unidades_orgao.sequencialOrgao,
        )
        .group_by(
            [
                silver_contratacoes.anoContratacao,
                silver_contratacoes.modalidadeNome,
                silver_dim_unidades_orgao.nomeOrgao,
            ]
        )
        .agg(
            quantidade_contratacoes=_.numeroControlePNCP.count(),
            valor_total_estimado=_.valorTotalEstimado.sum(),
        )
    )


def create_gold_deduplication_efficiency(
    bronze_pncp_raw: ibis.Table, silver_contratacoes: ibis.Table
) -> ibis.Table:
    raw_counts = bronze_pncp_raw.agg(raw_row_count=_.count())
    deduplicated_counts = silver_contratacoes.agg(
        deduplicated_row_count=_.count()
    )
    return raw_counts.cross_join(deduplicated_counts).mutate(
        deduplication_percentage=(
            1
            - (
                _.deduplicated_row_count.cast("float")
                / _.raw_row_count.cast("float")
            )
        )
        * 100
    )


def create_mart_compras_beneficios(
    silver_fact_contratacoes: ibis.Table,
    silver_itens_contratacao: ibis.Table,
    mart_procurement_analytics: ibis.Table,
) -> ibis.Table:
    beneficios_agregados = (
        silver_fact_contratacoes.join(
            silver_itens_contratacao,
            silver_fact_contratacoes.numero_controle_pncp
            == silver_itens_contratacao.numero_controle_pncp,
        )
        .filter(_.modalidade_id.isin([1, 2, 3]))
        .group_by(silver_fact_contratacoes.org_key)
        .agg(
            valor_total_com_beneficio=_.valor_total.sum(),
            qtd_itens_com_beneficio=_.numero_item.count(),
            qtd_total_itens=silver_itens_contratacao.count(),
        )
    )
    return beneficios_agregados.join(
        mart_procurement_analytics, "org_key"
    ).mutate(
        percentual_itens_com_beneficio=(
            _.qtd_itens_com_beneficio * 100.0 / _.qtd_total_itens
        ),
        percentual_valor_com_beneficio=(
            _.valor_total_com_beneficio / _.total_estimated_value
        ),
    )


def create_mart_procurement_analytics(
    silver_fact_contratacoes: ibis.Table, silver_fact_contratos: ibis.Table
) -> ibis.Table:
    procurement_analytics = (
        silver_fact_contratacoes.group_by("org_key")
        .agg(
            total_procurements=_.procurement_key.nunique(),
            total_estimated_value=_.valor_total_estimado.sum(),
            avg_proposal_duration=_.duracao_proposta_dias.mean(),
        )
    )
    contract_analytics = (
        silver_fact_contratos.group_by("org_key")
        .agg(
            total_contracts=_.contract_key.nunique(),
            total_contract_value=_.valor_inicial.sum(),
            avg_contract_duration=_.duracao_vigencia_dias.mean(),
        )
    )
    return procurement_analytics.join(contract_analytics, "org_key")
