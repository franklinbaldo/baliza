from kedro.pipeline import Pipeline, node
from .gold_nodes import (
    create_gold_contratacoes_analytics,
    create_gold_deduplication_efficiency,
    create_mart_compras_beneficios,
    create_mart_procurement_analytics,
)

def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=create_gold_contratacoes_analytics,
                inputs=["silver_contratacoes", "silver_dim_unidades_orgao"],
                outputs="gold_contratacoes_analytics",
                name="create_gold_contratacoes_analytics",
            ),
            node(
                func=create_gold_deduplication_efficiency,
                inputs=["bronze_pncp_raw", "silver_contratacoes"],
                outputs="gold_deduplication_efficiency",
                name="create_gold_deduplication_efficiency",
            ),
            node(
                func=create_mart_compras_beneficios,
                inputs=[
                    "silver_fact_contratacoes",
                    "silver_itens_contratacao",
                    "mart_procurement_analytics",
                ],
                outputs="mart_compras_beneficios",
                name="create_mart_compras_beneficios",
            ),
            node(
                func=create_mart_procurement_analytics,
                inputs=["silver_fact_contratacoes", "silver_fact_contratos"],
                outputs="mart_procurement_analytics",
                name="create_mart_procurement_analytics",
            ),
        ]
    )
