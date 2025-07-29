# src/baliza/pipeline.py

"""
Pipeline principal do Baliza - Refatorado com dlt best practices.
"""

from typing import Optional, Generator
import copy
import dlt
from dlt.extract.source import DltResource
from dlt.sources.rest_api import rest_api_source
from datetime import datetime

from .utils.time import date_range_slicer
from .resources import create_pncp_rest_config
from .models import (
    RecuperarContratoDTO,
    AtaRegistroPrecoPeriodoDTO,
    RecuperarCompraPublicacaoDTO,
)

# =============================================================================
# FONTE PRINCIPAL PARA SINCRONIZAÇÃO CONTÍNUA (INCREMENTAL)
# =============================================================================

@dlt.source(name="baliza_source")
def pncp_source(
    resource_type: str = "backfill", base_url: str = "https://pncp.gov.br/api/consulta"
) -> DltResource:
    """
    Fonte dlt declarativa para a API do PNCP.
    Utiliza a configuração moderna do dlt rest_api_source.
    """
    api_config = create_pncp_rest_config(resource_type, base_url)
    source = rest_api_source(api_config)

    if resource_type == "backfill":
        for resource in source.resources.values():
            if "contratacoes" in resource.name:
                resource.apply_hints(columns=RecuperarCompraPublicacaoDTO)
            elif "contratos" in resource.name:
                resource.apply_hints(columns=RecuperarContratoDTO)
            elif "atas" in resource.name:
                resource.apply_hints(columns=AtaRegistroPrecoPeriodoDTO)

    return source


# =============================================================================
# RESOURCE PARA BACKFILL HISTÓRICO PARALELO
# =============================================================================

@dlt.resource(name="pncp_backfill", parallelized=True, write_disposition="merge")
def pncp_backfill_resource(
    start_date_str: str,
    end_date_str: str,
    chunk_days: int = 30,
    base_url: str = "https://pncp.gov.br/api/consulta",
):
    """
    Resource de backfill que executa a extração histórica em fatias
    paralelas e de forma resumível, usando o estado do dlt.
    """
    state = dlt.current.resource_state()
    completed_chunks = state.setdefault("completed_chunks", [])

    start_dt = datetime.strptime(start_date_str, "%Y%m%d")
    end_dt = datetime.strptime(end_date_str, "%Y%m%d")

    # Iterate over each date chunk
    for start_chunk_dt, end_chunk_dt in date_range_slicer(start_dt, end_dt, chunk_days):
        start_chunk_str = start_chunk_dt.strftime("%Y%m%d")
        end_chunk_str = end_chunk_dt.strftime("%Y%m%d")

        chunk_id = f"{start_chunk_str}-{end_chunk_str}"

        if chunk_id in completed_chunks:
            print(f"⏩ Pulando fatia já concluída: {chunk_id}")
            continue

        # Create a source for this specific chunk
        source = pncp_source(
            resource_type="backfill",
            base_url=base_url
        )

        for resource in source.resources.values():
            resource.endpoint["params"]["dataInicial"] = start_chunk_str
            resource.endpoint["params"]["dataFinal"] = end_chunk_str

        # Yield the data from the chunk-specific source
        print(f"📦 Processando fatia: {chunk_id}")
        yield from source

        # Mark chunk as completed in the state
        completed_chunks.append(chunk_id)
        print(f"✅ Fatia concluída: {chunk_id}")


# =============================================================================
# TRANSFORMATION RESOURCE
# =============================================================================

@dlt.resource(name="final_views", write_disposition="skip")
def create_consolidated_views(conn: "dlt.destinations.duckdb.DuckDbClient.get_connection" = dlt.secrets.value):
    """
    A dlt resource that runs after data loading to create
    our final, unified views in DuckDB.
    """
    view_sql = """
    CREATE OR REPLACE VIEW v_contratos_recentes AS
    -- The conciliation logic we designed
    WITH all_versions AS (
        SELECT *, dataPublicacaoPncp as version_timestamp FROM contratos_publicacao
        UNION ALL
        SELECT *, dataAtualizacaoGlobal as version_timestamp FROM contratos_atualizacao
    ),
    ranked_versions AS (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY numeroControlePNCP ORDER BY version_timestamp DESC) as rn
        FROM all_versions
    )
    SELECT * FROM ranked_versions WHERE rn = 1;
    """
    with conn.cursor() as cur:
        print("🚀 Creating/Replacing consolidated view: v_contratos_recentes")
        cur.execute(view_sql)
        # Repeat for other resources like 'contratacoes', 'atas', etc.

    yield {"status": "success", "view_name": "v_contratos_recentes", "timestamp": datetime.now()}


# =============================================================================
# FUNÇÕES DE EXECUÇÃO DO PIPELINE (sem alterações, mas incluídas para contexto)
# =============================================================================

def run_backfill_pipeline(
    start_date: str,
    end_date: str,
    chunk_days: int = 30,
    pipeline_name: str = "baliza_pncp_backfill",
    destination: str = "duckdb",
    dataset_name: str = "pncp_data",
):
    """Executa o pipeline de backfill histórico."""
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name=dataset_name,
        progress="log",
    )
    backfill_res = pncp_backfill_resource(start_date, end_date, chunk_days)
    info = pipeline.run(backfill_res)
    print(info)
    return info

def run_transformation_pipeline(
    pipeline_name: str = "baliza_pncp_transform",
    destination: str = "duckdb",
    dataset_name: str = "pncp_data",
):
    """Executa o pipeline de transformação."""
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name=dataset_name,
        progress="log",
    )
    info = pipeline.run(create_consolidated_views())
    print(info)
    return info
