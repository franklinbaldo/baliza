import ibis
from pipelines.ibis_nodes import (
    transform_silver_contratacoes,
    create_gold_contratacoes_analytics,
)

def run_ibis_pipeline(con: ibis.BaseBackend):
    """
    Runs the Ibis transformation pipeline.
    """
    # Silver layer
    silver_contratacoes = transform_silver_contratacoes(con)
    con.create_table("silver_contratacoes", silver_contratacoes, overwrite=True)

    silver_dim_unidades_orgao = transform_silver_dim_unidades_orgao(con)
    con.create_table(
        "silver_dim_unidades_orgao", silver_dim_unidades_orgao, overwrite=True
    )

    # Gold layer
    gold_contratacoes_analytics = create_gold_contratacoes_analytics(
        silver_contratacoes, silver_dim_unidades_orgao
    )
    con.create_table(
        "gold_contratacoes_analytics", gold_contratacoes_analytics, overwrite=True
    )

if __name__ == "__main__":
    # This allows running the pipeline directly for testing
    # In the actual application, the CLI will call run_ibis_pipeline
    con = ibis.connect("duckdb://baliza.db")
    run_ibis_pipeline(con)
    print("Ibis pipeline executed successfully.")
