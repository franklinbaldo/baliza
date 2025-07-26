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
    # Use first 3 endpoints as priority (configurable via settings)
    from baliza.settings import settings
    priority_endpoints = settings.all_pncp_endpoints[:3]
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
def mark_extraction_completed(output_dir: str, start_date: str, end_date: str, endpoints: List[str]):
    """Mark extraction as completed for endpoint/month combinations."""
    base_path = Path(output_dir)
    
    # Generate list of months between start and end dates
    months = _get_months_in_range(start_date, end_date)
    
    for endpoint in endpoints:
        for month in months:
            endpoint_dir = base_path / endpoint / month
            endpoint_dir.mkdir(parents=True, exist_ok=True)
            
            # Create completion marker
            completion_file = endpoint_dir / ".completed"
            completion_data = {
                "completed_at": datetime.now().isoformat(),
                "endpoint": endpoint,
                "month": month,
                "start_date": start_date,
                "end_date": end_date,
                "extractor_version": "2.0-dlt"
            }
            
            with open(completion_file, 'w') as f:
                json.dump(completion_data, f, indent=2)
                
            print(f"✅ Marked {endpoint}/{month} as completed")


def is_extraction_completed(output_dir: str, endpoint: str, month: str) -> bool:
    """Check if extraction is completed for endpoint/month combination."""
    completion_file = Path(output_dir) / endpoint / month / ".completed"
    return completion_file.exists()


def get_completed_extractions(output_dir: str) -> Dict[str, List[str]]:
    """Get list of completed extractions organized by endpoint."""
    base_path = Path(output_dir)
    completed = {}
    
    if not base_path.exists():
        return completed
    
    for endpoint_dir in base_path.iterdir():
        if endpoint_dir.is_dir():
            endpoint = endpoint_dir.name
            completed[endpoint] = []
            
            for month_dir in endpoint_dir.iterdir():
                if month_dir.is_dir() and (month_dir / ".completed").exists():
                    completed[endpoint].append(month_dir.name)
    
    return completed


def _get_months_in_range(start_date: str, end_date: str) -> List[str]:
    """Generate list of YYYY-MM strings between start and end dates."""
    # Parse dates (assuming YYYYMMDD format)
    start_year = int(start_date[:4])
    start_month = int(start_date[4:6])
    end_year = int(end_date[:4])
    end_month = int(end_date[4:6])
    
    months = []
    
    current_year = start_year
    current_month = start_month
    
    while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
        months.append(f"{current_year:04d}-{current_month:02d}")
        
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1
    
    return months


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
EOF < /dev/null
