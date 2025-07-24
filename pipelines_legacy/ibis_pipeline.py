import ibis
import time
from typing import Dict, Any
from pipelines.ibis_nodes import (
    transform_stage_contratacoes,
    transform_stage_dim_unidades_orgao,
    create_mart_contratacoes_analytics,
)
from pipelines.domain_nodes import (
    load_domain_tables,
    enrich_with_domain_data,
    get_domain_table_stats
)

def run_ibis_pipeline(con: ibis.BaseBackend, enable_domain_enrichment: bool = True) -> Dict[str, Any]:
    """
    Runs the enhanced Ibis transformation pipeline with domain table integration.
    Uses the Raw → Stage → Mart medallion architecture.
    
    Args:
        con: Ibis backend connection
        enable_domain_enrichment: Whether to load and use domain tables for enrichment
        
    Returns:
        Pipeline execution metrics
    """
    start_time = time.time()
    step_times = {}
    
    print("🦜 Starting Enhanced Ibis Pipeline (Raw → Stage → Mart)")
    
    # 1. Load domain reference tables (if enabled)
    domain_tables = {}
    if enable_domain_enrichment:
        step_start = time.time()
        print("📋 Loading domain tables...")
        domain_tables = load_domain_tables(con)
        step_times["domain_loading"] = time.time() - step_start
        print(f"✅ Domain tables loaded: {len(domain_tables)} tables ({step_times['domain_loading']:.2f}s)")
    
    # 2. Stage layer transformations
    step_start = time.time()
    print("🔄 Processing Stage layer...")
    
    stage_contratacoes = transform_stage_contratacoes(con)
    
    # Enrich with domain data if enabled
    if enable_domain_enrichment and domain_tables:
        stage_contratacoes_enriched = enrich_with_domain_data(stage_contratacoes, con)
        con.create_table("stage_contratacoes", stage_contratacoes_enriched, overwrite=True)
    else:
        con.create_table("stage_contratacoes", stage_contratacoes, overwrite=True)

    stage_dim_unidades_orgao = transform_stage_dim_unidades_orgao(con)
    con.create_table("stage_dim_unidades_orgao", stage_dim_unidades_orgao, overwrite=True)
    
    step_times["stage_layer"] = time.time() - step_start
    print(f"✅ Stage layer complete ({step_times['stage_layer']:.2f}s)")

    # 3. Mart layer analytics
    step_start = time.time()
    print("📊 Processing Mart layer...")
    
    # Use enriched stage table for mart layer
    final_stage_contratacoes = con.table("stage_contratacoes")
    mart_contratacoes_analytics = create_mart_contratacoes_analytics(
        final_stage_contratacoes, stage_dim_unidades_orgao
    )
    con.create_table("mart_contratacoes_analytics", mart_contratacoes_analytics, overwrite=True)
    
    step_times["mart_layer"] = time.time() - step_start
    print(f"✅ Mart layer complete ({step_times['mart_layer']:.2f}s)")
    
    # 4. Generate metrics
    total_time = time.time() - start_time
    
    # Get domain table stats
    domain_stats = get_domain_table_stats(con) if enable_domain_enrichment else {}
    
    metrics = {
        "total_time": total_time,
        "step_times": step_times,
        "tables_created": len(con.list_tables()),
        "domain_enrichment_enabled": enable_domain_enrichment,
        "domain_tables_loaded": len(domain_tables),
        "domain_stats": domain_stats
    }
    
    print(f"🎉 Pipeline completed successfully in {total_time:.2f}s")
    print(f"📊 Tables created: {metrics['tables_created']}")
    if enable_domain_enrichment:
        print(f"📋 Domain tables: {metrics['domain_tables_loaded']}")
    
    return metrics

if __name__ == "__main__":
    # This allows running the pipeline directly for testing
    # In the actual application, the CLI will call run_ibis_pipeline
    con = ibis.connect("duckdb://baliza.db")
    run_ibis_pipeline(con)
    print("Ibis pipeline executed successfully.")
