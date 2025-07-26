"""
PNCP REST API Configuration for DLT
Converts legacy ENDPOINT_CONFIG to dlt REST API format
"""

from datetime import date, timedelta
from typing import Dict, Any, List
from baliza.settings import ENDPOINT_CONFIG, settings
from baliza.schemas import ModalidadeContratacao
from baliza.utils import hash_sha256


def create_pncp_rest_config(
    start_date: str = None,
    end_date: str = None,
    modalidades: List[int] = None
) -> Dict[str, Any]:
    """
    Create dlt REST API configuration for PNCP endpoints.
    
    Args:
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format  
        modalidades: List of modalidade IDs to process
    
    Returns:
        RESTAPIConfig dict ready for use with rest_api_source()
    """
    
    # Default date range if not provided
    if not start_date or not end_date:
        end_dt = date.today()
        start_dt = end_dt - timedelta(days=settings.default_date_range_days)
        start_date = start_dt.strftime("%Y%m%d")
        end_date = end_dt.strftime("%Y%m%d")
    
    # Client configuration
    # Dynamic User-Agent with version info
    try:
        from importlib.metadata import version
        baliza_version = version("baliza")
    except ImportError:
        baliza_version = "2.0.0-dev"
    
    client_config = {
        "base_url": settings.pncp_api_base_url,
        "headers": {
            "User-Agent": f"Baliza/{baliza_version} DLT Pipeline",
            "Accept": "application/json"
        }
        # Note: timeout would need to be configured through session if needed
        # Note: DLT doesn't provide request-level caching, so we implement deduplication at data level
    }
    
    # Build resources from ENDPOINT_CONFIG
    resources = []
    
    for endpoint_name, endpoint_config in ENDPOINT_CONFIG.items():
        # Process ALL endpoints - no phase restrictions
            
        # Get appropriate page size for this endpoint
        page_size = settings.ENDPOINT_PAGE_LIMITS.get(endpoint_name, settings.default_page_size)
        
        # Base resource configuration
        resource = {
            "name": endpoint_name,
            "endpoint": {
                "path": endpoint_config.path,
                "method": "GET",
                "params": _build_endpoint_params(
                    endpoint_config, start_date, end_date, modalidades, page_size
                ),
                "paginator": _get_paginator_config(endpoint_config),
                "data_selector": "data",  # PNCP responses have data array
            },
            "primary_key": "_dlt_id",  # Will be added by processing step
            "write_disposition": "merge",  # Deduplication based on hash
            "processing_steps": [
                {
                    "map": _add_hash_id  # Map function for deduplication
                },
                {
                    "map": _add_metadata  # Map function for metadata
                }
            ]
            # Note: Incremental loading handled by gap detection instead of DLT incremental
        }
        
        resources.append(resource)
    
    return {
        "client": client_config,
        "resources": resources
    }


def _build_endpoint_params(endpoint_config, start_date: str, end_date: str, modalidades: List[int], page_size: int = None) -> Dict[str, Any]:
    """Build parameters for an endpoint based on its configuration."""
    
    # Use provided page_size or fallback to endpoint default
    effective_page_size = page_size if page_size is not None else endpoint_config.default_page_size
    
    params = {
        "tamanhoPagina": effective_page_size,
        "pagina": 1,  # Will be handled by paginator
    }
    
    # Add date parameters if required
    if "dataInicial" in endpoint_config.required_params:
        params["dataInicial"] = start_date
    if "dataFinal" in endpoint_config.required_params:
        params["dataFinal"] = end_date
        
    # Add modalidade if required and provided
    if endpoint_config.requires_modalidade and modalidades:
        # Note: This function builds params for a single resource. For multiple modalidades,
        # the pipeline creates separate resources using create_modalidade_resources().
        # Here we use the first modalidade as this function is called per resource.
        params["codigoModalidadeContratacao"] = modalidades[0]
    
    return params


def _get_paginator_config(endpoint_config) -> Dict[str, Any]:
    """Get appropriate paginator configuration for endpoint."""
    
    # PNCP uses page number pagination with custom field names
    return {
        "type": "page_number",
        "page_param": "pagina",           # PNCP parameter name
        "total_path": "totalPaginas",     # PNCP field for total pages
        "base_page": 1,                   # PNCP starts at page 1
        "stop_after_empty_page": True     # Stop if no data returned
    }


def _add_hash_id(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Processing step to add hash-based ID for deduplication.
    Uses legacy hash function to maintain compatibility.
    """
    # Create a copy to avoid mutating original
    record_copy = record.copy()
    
    # Add hash-based ID using legacy function
    record_copy["_dlt_id"] = hash_sha256(record)
    
    return record_copy


# Note: Additional processing functions could be added here if needed for future DLT enhancements

def _add_metadata(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Processing step to add extraction metadata.
    """
    record_copy = record.copy()
    
    # Add extraction timestamp
    record_copy["_baliza_extracted_at"] = date.today().isoformat()
    
    # Note: URL will be added by dlt automatically from request context
    # We don't need to manually add it here
    
    return record_copy




# Removed create_modalidade_resources() - functionality integrated into pncp_source()


# Configuration for common use cases
def get_default_config() -> Dict[str, Any]:
    """Get default configuration for testing and development."""
    return create_pncp_rest_config()


def get_production_config(start_date: str, end_date: str) -> Dict[str, Any]:
    """Get production configuration with all modalidades."""
    all_modalidades = [m.value for m in ModalidadeContratacao]
    return create_pncp_rest_config(start_date, end_date, all_modalidades)


def get_priority_endpoints_config(start_date: str, end_date: str) -> Dict[str, Any]:
    """Get configuration for priority endpoints only (Phase 2a)."""
    priority_modalidades = [
        ModalidadeContratacao.PREGAO_ELETRONICO.value,
        ModalidadeContratacao.PREGAO_PRESENCIAL.value,
        ModalidadeContratacao.CONCORRENCIA_ELETRONICA.value,
    ]
    return create_pncp_rest_config(start_date, end_date, priority_modalidades)