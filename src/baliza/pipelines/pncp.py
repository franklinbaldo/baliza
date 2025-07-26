"""
PNCP Data Pipeline using DLT REST API Source
Pure configuration-driven approach with zero custom HTTP code

Note on Request-Level Deduplication:
- DLT provides data-level deduplication (hash-based, primary key-based)
- DLT does NOT provide HTTP request-level deduplication/caching
- Our hash-based deduplication prevents saving duplicates but doesn't prevent HTTP requests
- For production use, consider implementing request caching at the infrastructure level
  (e.g., HTTP cache, Redis, or API gateway caching)
"""

import dlt
from dlt.sources.rest_api import rest_api_source
from typing import List, Optional, Any
from .pncp_config import create_pncp_rest_config, create_modalidade_resources
from baliza.legacy.enums import ModalidadeContratacao


def pncp_source(
    start_date: str = None,
    end_date: str = None,
    modalidades: List[int] = None,
    endpoints: List[str] = None
):
    """
    Create PNCP data source using dlt REST API built-ins with incremental loading.
    DLT's incremental loading prevents re-requesting data we already have.
    
    Args:
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format  
        modalidades: List of modalidade IDs to process
        endpoints: List of endpoint names to include (default: priority endpoints)
    
    Returns:
        DLT source ready for pipeline execution
    """
    
    # Create REST API configuration with incremental loading
    config = create_pncp_rest_config(start_date, end_date, modalidades)
    
    # Filter endpoints if specified
    if endpoints:
        config["resources"] = [
            r for r in config["resources"] 
            if r["name"] in endpoints
        ]
    
    # Create and return dlt REST API source with incremental loading
    # The incremental configuration in the config will handle request-level deduplication
    return rest_api_source(config, name="pncp")


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