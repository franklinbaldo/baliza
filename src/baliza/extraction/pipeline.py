"""
PNCP Data Pipeline using DLT REST API Source
Pure configuration-driven approach with zero custom HTTP code

Smart Gap Detection:
- Uses DLT state to detect what data we already have
- Only fetches missing date ranges to avoid redundant requests
- Provides true incremental extraction at the request level
"""

import dlt
from dlt.sources.rest_api import rest_api_source
from dlt.destinations import filesystem
from typing import List, Any
from .config import create_pncp_rest_config
from .gap_detector import find_extraction_gaps
from baliza.schemas import ModalidadeContratacao
from baliza.utils.completion_tracking import (
    mark_extraction_completed,
)


def pncp_source(
    start_date: str = None,
    end_date: str = None,
    modalidades: List[int] = None,
    endpoints: List[str] = None,
    backfill_all: bool = False,
    return_all_sources: bool = False,
):
    """
    Create PNCP data source with smart gap detection.
    Only fetches data for missing date ranges - true incremental extraction!

    Args:
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        modalidades: List of modalidade IDs to process
        endpoints: List of endpoint names to include (default: priority endpoints)
        backfill_all: If True, fetch all historical data gaps
        return_all_sources: If True, return list of all sources for monthly processing

    Returns:
        DLT source ready for pipeline execution, or list of sources if return_all_sources=True
    """

    # Find gaps in existing data
    gaps = find_extraction_gaps(
        start_date=start_date,
        end_date=end_date,
        endpoints=endpoints,
        backfill_all=backfill_all,
    )

    # If no gaps, return empty source
    if not gaps:
        return _empty_pncp_source()

    # Create sources for each gap
    sources = []
    
    for gap in gaps:
        # Determine modalidades for this gap
        gap_modalidades = None
        if gap.modalidade:
            # Single modalidade for this gap
            gap_modalidades = [gap.modalidade]
        elif gap.missing_pages:
            # Specific pages needed - create targeted requests
            gap_modalidades = modalidades
        else:
            # Full date range needed - fetch all pages
            gap_modalidades = modalidades

        config = create_pncp_rest_config(gap.start_date, gap.end_date, gap_modalidades)

        # Filter to only this endpoint
        config["resources"] = [
            r for r in config["resources"] if r["name"] == gap.endpoint
        ]

        if config["resources"]:
            # Create unique source name including modalidade if present
            source_name = f"pncp_{gap.endpoint}_{gap.start_date}"
            if gap.modalidade:
                source_name += f"_mod{gap.modalidade}"
            
            gap_source = rest_api_source(config, name=source_name)
            sources.append(gap_source)

    # Handle return format based on parameter
    if not sources:
        return [_empty_pncp_source()] if return_all_sources else _empty_pncp_source()
    
    if return_all_sources:
        # Return all sources for monthly processing - each month is independent
        return sources
    else:
        # Legacy behavior - return single source (for existing code compatibility)
        return sources[0]


def _empty_pncp_source():
    """Return an empty source when no data needs to be fetched."""

    @dlt.source
    def empty_pncp():
        """Empty source for when all data already exists."""
        return []

    return empty_pncp()


def pncp_monthly_sources(
    start_date: str = None,
    end_date: str = None,
    endpoints: List[str] = None,
    backfill_all: bool = False,
):
    """
    Create multiple PNCP sources for monthly extraction.
    Each month becomes an independent DLT source for better isolation and error handling.
    
    Args:
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        endpoints: List of endpoint names to include
        backfill_all: If True, fetch all historical monthly gaps from 2021-01
        
    Returns:
        List of DLT sources, one per month
    """
    # TODO: Revisit this function. The `pncp_source` now returns a list of sources
    #       when `return_all_sources=True`. This pattern might be simplified if
    #       `pncp_source` itself could yield multiple resources directly, allowing
    #       DLT to manage the iteration over months/modalidades more natively.
    return pncp_source(
        start_date=start_date,
        end_date=end_date,
        endpoints=endpoints,
        backfill_all=backfill_all,
        return_all_sources=True
    )


def pncp_priority_source(start_date: str, end_date: str):
    """
    Source for priority endpoints only (Phase 2a implementation).
    Includes: contratacoes_publicacao, contratos, atas
    """
    from baliza.settings import settings

    priority_endpoints = settings.all_pncp_endpoints[:3]
    return pncp_source(start_date, end_date, endpoints=priority_endpoints)


def pncp_modalidade_source(
    start_date: str, end_date: str, modalidade: ModalidadeContratacao
):
    """
    Source for a specific modalidade with separate resources per modalidade.
    Useful for endpoints that require modalidade parameter.

    Note: This function provides modalidade-specific extraction.
    Can be replaced by run_structured_extraction with modalidade parameter.
    """
    return pncp_source(
        start_date=start_date,
        end_date=end_date,
        modalidades=[modalidade.value],
        endpoints=["contratacoes_publicacao"],  # Only endpoint requiring modalidade
    )


def pncp_all_modalidades_source(start_date: str, end_date: str):
    """
    Source for all modalidades - creates separate resources for each.
    Production use case for complete data extraction.
    """
    all_modalidades = [m.value for m in ModalidadeContratacao]
    return pncp_source(start_date, end_date, modalidades=all_modalidades)


# Convenience functions for common use cases
def create_default_pipeline(destination: str = "parquet", output_dir: str = "data", progress: str = "enlighten"):
    """Create structured pipeline with Parquet export by endpoint and month.
    
    Args:
        destination: Destination type (default: "parquet")
        output_dir: Output directory for data (default: "data")
        progress: Progress tracking mode - enlighten, tqdm, alive_progress, or log (default: "enlighten")
    """
    if destination == "parquet":
        # Use filesystem destination for structured Parquet export
        dest = filesystem(bucket_url=output_dir, layout="{table_name}/{load_id}")
        return dlt.pipeline(
            pipeline_name="baliza_pncp", 
            destination=dest, 
            dataset_name="pncp_raw",
            progress=progress
        )
    else:
        return dlt.pipeline(
            pipeline_name="baliza_pncp",
            destination=destination,
            dataset_name="pncp_data",
            progress=progress
        )


def run_priority_extraction(
    start_date: str,
    end_date: str,
    destination: str = "parquet",
    output_dir: str = "data",
    progress: str = "enlighten",
) -> Any:
    """
    Run extraction for priority endpoints with structured output.

    Note: This function provides priority endpoint extraction.
    Can be replaced by run_structured_extraction with endpoint filtering.

    Returns:
        Pipeline run summary with metrics
    """
    pipeline = create_default_pipeline(destination, output_dir, progress)
    source = pncp_priority_source(start_date, end_date)

    # Run the pipeline - dlt handles everything!
    result = pipeline.run(source)

    # Mark extraction as completed for each endpoint and month
    if destination == "parquet":
        mark_extraction_completed(
            output_dir,
            start_date,
            end_date,
            ["contratacoes_publicacao", "contratos", "atas"],
        )

    return result


def run_modalidade_extraction(
    start_date: str,
    end_date: str,
    modalidade: ModalidadeContratacao,
    destination: str = "duckdb",
    progress: str = "enlighten",
) -> Any:
    """
    Run extraction for specific modalidade.

    Note: This function provides modalidade-specific extraction.
    Can be replaced by run_structured_extraction with modalidade parameter.

    Returns:
        Pipeline run summary with metrics
    """
    pipeline = create_default_pipeline(destination, progress=progress)
    source = pncp_modalidade_source(start_date, end_date, modalidade)

    return pipeline.run(source)


# Migration compatibility layer (temporary)
def pncp_source_legacy_compat(
    start_date: str = "20240101",
    end_date: str = "20240101", 
    modalidade: int = None,
    extractor_instance = None  # Ignored - no longer needed!
):
    """
    Legacy compatibility wrapper for existing code.
    
    TODO: Evaluate if this legacy compatibility function is still necessary.
    If there are no external callers or deprecated parts of the system relying
    on this, it should be removed to reduce code complexity and maintenance burden.
    
    WARNING: This is deprecated. Use pncp_source() directly.
    extractor_instance parameter is ignored (no longer needed with dlt built-ins).
    """
    modalidades = [modalidade] if modalidade else None
    return pncp_source(start_date, end_date, modalidades)
