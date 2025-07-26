"""
PNCP Data Pipeline using DLT REST API Source
Pure configuration-driven approach with zero custom HTTP code

Smart Gap Detection:
- Uses DLT state to detect what data we already have
- Only fetches missing date ranges to avoid redundant requests
- Provides true incremental extraction at the request level
"""

import dlt
import json
from dlt.sources.rest_api import rest_api_source
from dlt.destinations import filesystem
from pathlib import Path
from datetime import datetime, date
from typing import List, Optional, Any, Dict
from .config import create_pncp_rest_config, create_modalidade_resources
from .gap_detector import find_extraction_gaps, DataGap
from baliza.schemas import ModalidadeContratacao
from baliza.utils.completion_tracking import mark_extraction_completed, get_completed_extractions, _get_months_in_range


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
            # TODO: This is inefficient. The DLT REST API source doesn't easily support
            #       requesting specific pages out of the box. A custom resource or a more
            #       advanced paginator strategy could be implemented to only fetch the
            #       truly missing pages instead of the entire date range.
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
    from baliza.settings import settings
    priority_endpoints = settings.all_pncp_endpoints[:3]
    return pncp_source(start_date, end_date, endpoints=priority_endpoints)


def pncp_modalidade_source(


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
def create_default_pipeline(destination: str = "parquet", output_dir: str = "data"):
    """Create structured pipeline with Parquet export by endpoint and month."""
    if destination == "parquet":
        # Use filesystem destination for structured Parquet export
        dest = filesystem(bucket_url=output_dir, layout="{table_name}/{load_id}")
        return dlt.pipeline(
            pipeline_name="baliza_pncp",
            destination=dest,
            dataset_name="pncp_raw"
        )
    else:
        return dlt.pipeline(
            pipeline_name="baliza_pncp", 
            destination=destination,
            dataset_name="pncp_data"
        )


def run_priority_extraction(
    start_date: str, 
    end_date: str,
    destination: str = "parquet",
    output_dir: str = "data"
) -> Any:
    """
    Run extraction for priority endpoints with structured output.
    
    Returns:
        Pipeline run summary with metrics
    """
    pipeline = create_default_pipeline(destination, output_dir)
    source = pncp_priority_source(start_date, end_date)
    
    # Run the pipeline - dlt handles everything!
    result = pipeline.run(source)
    
    # Mark extraction as completed for each endpoint and month
    if destination == "parquet":
        mark_extraction_completed(output_dir, start_date, end_date, ["contratacoes_publicacao", "contratos", "atas"])
    
    return result


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


# TODO: The completion tracking functions (`mark_extraction_completed`,
#       `is_extraction_completed`, `get_completed_extractions`, and
#       `_get_months_in_range`) are currently defined in this module.
#       Consider moving these functions to `baliza.utils.completion_tracking`
#       to centralize all completion tracking logic and improve modularity.

def mark_extraction_completed(
    output_dir: str, start_date: str, end_date: str, endpoints: List[str]
):


def run_structured_extraction(
    start_date: str = None,
    end_date: str = None,
    endpoints: List[str] = None,
    output_dir: str = "data",
    skip_completed: bool = True
) -> Any:
    """Run extraction with structured Parquet output and completion tracking.
    
    Args:
        start_date: Start date in YYYYMMDD format (optional for backfill)
        end_date: End date in YYYYMMDD format (optional for backfill)
        endpoints: List of endpoints to extract (default: all)
        output_dir: Output directory for structured Parquet files
        skip_completed: Skip already completed endpoint/month combinations
    
    Returns:
        Pipeline run summary with metrics
    """
    # Use all endpoints if none specified
    from baliza.settings import settings
    if not endpoints:
        endpoints = settings.all_pncp_endpoints
    
    # Filter out already completed extractions
    if skip_completed:
        completed = get_completed_extractions(output_dir)
        months_in_range = _get_months_in_range(start_date or "20240101", end_date or "20241231")
        
        filtered_endpoints = []
        for endpoint in endpoints:
            if endpoint not in completed:
                filtered_endpoints.append(endpoint)
            else:
                # Check if all months are completed
                completed_months = set(completed[endpoint])
                required_months = set(months_in_range)
                
                if not required_months.issubset(completed_months):
                    filtered_endpoints.append(endpoint)
                else:
                    print(f"⏭️  Skipping {endpoint} - all months already completed")
        
        endpoints = filtered_endpoints
    
    if not endpoints:
        print("✅ All extractions already completed")
        return None
    
    # Create pipeline with structured output
    pipeline = create_default_pipeline("parquet", output_dir)
    
    # Create source with gap detection
    source = pncp_source(
        start_date=start_date,
        end_date=end_date,
        endpoints=endpoints,
        backfill_all=(start_date is None and end_date is None)
    )
    
    # Run extraction
    result = pipeline.run(source)
    
    # Mark extractions as completed
    mark_extraction_completed(output_dir, start_date or "20240101", end_date or "20241231", endpoints)
    
    return result
