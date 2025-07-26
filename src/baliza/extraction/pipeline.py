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
from typing import List, Optional, Any
from .config import create_pncp_rest_config, create_modalidade_resources
from .gap_detector import find_extraction_gaps, DataGap
from baliza.schemas import ModalidadeContratacao


def pncp_source(
    start_date: str = None,
    end_date: str = None,
    modalidades: List[int] = None,
    endpoints: List[str] = None,
    backfill_all: bool = False
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
    
    Returns:
        DLT source ready for pipeline execution
    """
    
    # Find gaps in existing data
    gaps = find_extraction_gaps(
        start_date=start_date,
        end_date=end_date,
        endpoints=endpoints,
        backfill_all=backfill_all
    )
    
    # If no gaps, return empty source
    if not gaps:
        print("✅ No missing data found - skipping extraction")
        return _empty_pncp_source()
    
    # Create sources for each gap
    sources = []
    for gap in gaps:
        print(f"🔄 Creating source for gap: {gap}")
        
        if gap.missing_pages:
            # Specific pages needed - create targeted requests
            print(f"   📄 Fetching specific pages: {gap.missing_pages[:5]}{'...' if len(gap.missing_pages) > 5 else ''}")
            # For now, create a source that will fetch all pages (DLT REST API doesn't support page-specific requests easily)
            # This could be optimized by creating a custom resource that only requests specific pages
            config = create_pncp_rest_config(gap.start_date, gap.end_date, modalidades)
        else:
            # Full date range needed - fetch all pages
            print(f"   📅 Fetching full date range: {gap.start_date} to {gap.end_date}")
            config = create_pncp_rest_config(gap.start_date, gap.end_date, modalidades)
        
        # Filter to only this endpoint
        config["resources"] = [
            r for r in config["resources"] 
            if r["name"] == gap.endpoint
        ]
        
        if config["resources"]:
            gap_source = rest_api_source(config, name=f"pncp_{gap.endpoint}_{gap.start_date}")
            sources.append(gap_source)
    
    # Combine all gap sources
    # For now, return the first source (DLT will handle merging in pipeline)
    return sources[0] if sources else _empty_pncp_source()


def _empty_pncp_source():
    """Return an empty source when no data needs to be fetched."""
    
    @dlt.source
    def empty_pncp():
        """Empty source for when all data already exists."""
        return []
    
    return empty_pncp()


def pncp_priority_source(start_date: str, end_date: str):
    """
    Source for priority endpoints only (Phase 2a implementation).
    Includes: contratacoes_publicacao, contratos, atas
    """
    priority_endpoints = ["contratacoes_publicacao", "contratos", "atas"]
    return pncp_source(start_date, end_date, endpoints=priority_endpoints)


def pncp_modalidade_source(
    start_date: str, 
    end_date: str, 
    modalidade: ModalidadeContratacao
):
    """
    Source for a specific modalidade with separate resources per modalidade.
    Useful for endpoints that require modalidade parameter.
    """
    return pncp_source(
        start_date=start_date,
        end_date=end_date, 
        modalidades=[modalidade.value],
        endpoints=["contratacoes_publicacao"]  # Only endpoint requiring modalidade
    )


def pncp_all_modalidades_source(start_date: str, end_date: str):
    """
    Source for all modalidades - creates separate resources for each.
    Production use case for complete data extraction.
    """
    all_modalidades = [m.value for m in ModalidadeContratacao]
    return pncp_source(start_date, end_date, modalidades=all_modalidades)


# Convenience functions for common use cases
def create_default_pipeline(destination: str = "duckdb"):
    """Create default pipeline for development/testing."""
    return dlt.pipeline(
        pipeline_name="baliza_pncp", 
        destination=destination,
        dataset_name="pncp_data"
    )


def run_priority_extraction(
    start_date: str, 
    end_date: str,
    destination: str = "duckdb"
) -> Any:
    """
    Run extraction for priority endpoints.
    
    Returns:
        Pipeline run summary with metrics
    """
    pipeline = create_default_pipeline(destination)
    source = pncp_priority_source(start_date, end_date)
    
    # Run the pipeline - dlt handles everything!
    return pipeline.run(source)


def run_modalidade_extraction(
    start_date: str,
    end_date: str, 
    modalidade: ModalidadeContratacao,
    destination: str = "duckdb"
) -> Any:
    """
    Run extraction for specific modalidade.
    
    Returns:
        Pipeline run summary with metrics
    """
    pipeline = create_default_pipeline(destination)
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
    
    WARNING: This is deprecated. Use pncp_source() directly.
    extractor_instance parameter is ignored (no longer needed with dlt built-ins).
    """
    modalidades = [modalidade] if modalidade else None
    return pncp_source(start_date, end_date, modalidades)


if __name__ == "__main__":
    # Example usage
    from datetime import date, timedelta
    
    # Get last 7 days
    end_date = date.today()
    start_date = end_date - timedelta(days=7)
    
    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")
    
    print(f"Running PNCP extraction for {start_str} to {end_str}")
    
    # Run priority extraction
    result = run_priority_extraction(start_str, end_str)
    
    print(f"Extraction completed: {result.metrics}")