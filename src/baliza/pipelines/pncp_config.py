"""
PNCP REST API Configuration for DLT
Converts legacy ENDPOINT_CONFIG to dlt REST API format
"""

from datetime import date, timedelta
from typing import Dict, Any, List, Optional
from dlt.sources.rest_api.typing import RESTAPIConfig
from baliza.config import ENDPOINT_CONFIG, settings
from baliza.legacy.enums import PncpEndpoint, ModalidadeContratacao
from baliza.legacy.utils.hash_utils import hash_sha256
import os
import dlt


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
    client_config = {
        "base_url": settings.pncp_api_base_url,
        "headers": {
            "User-Agent": "Baliza/2.0 DLT Pipeline",
            "Accept": "application/json"
        }
        # Note: timeout would need to be configured through session if needed
        # Note: DLT doesn't provide request-level caching, so we implement deduplication at data level
    }
    
    # Build resources from ENDPOINT_CONFIG
    resources = []
    
    for endpoint_name, endpoint_config in ENDPOINT_CONFIG.items():
        # Skip endpoints not in our priority list for now
        if endpoint_name not in ["contratacoes_publicacao", "contratos", "atas"]:
            continue
            
        # Base resource configuration
        resource = {
            "name": endpoint_name,
            "endpoint": {
                "path": endpoint_config.path,
                "method": "GET",
                "params": _build_endpoint_params(
                    endpoint_config, start_date, end_date, modalidades
                ),
                "paginator": _get_paginator_config(endpoint_config),
                "data_selector": "data",  # PNCP responses have data array
            },
            "primary_key": "_dlt_id",  # Will be added by processing step
            "write_disposition": "merge",  # Deduplication based on hash
            # Add incremental loading to avoid re-requesting existing data
            "incremental": dlt.sources.incremental("_baliza_extracted_at"),
            "processing_steps": [
                {
                    "map": _add_hash_id  # Map function for deduplication
                },
                {
                    "map": _add_metadata  # Map function for metadata
                }
            ]
        }
        
        resources.append(resource)
    
    return {
        "client": client_config,
        "resources": resources
    }


def _build_endpoint_params(endpoint_config, start_date: str, end_date: str, modalidades: List[int]) -> Dict[str, Any]:
    """Build parameters for an endpoint based on its configuration."""
    
    params = {
        "tamanhoPagina": endpoint_config.default_page_size,
        "pagina": 1,  # Will be handled by paginator
    }
    
    # Add date parameters if required
    if "dataInicial" in endpoint_config.required_params:
        params["dataInicial"] = start_date
    if "dataFinal" in endpoint_config.required_params:
        params["dataFinal"] = end_date
        
    # Add modalidade if required and provided
    if endpoint_config.requires_modalidade and modalidades:
        # For endpoints requiring modalidade, we'll need to create multiple resources
        # For now, use the first modalidade
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


def _add_url_metadata(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Processing step to add URL metadata for request deduplication.
    This allows us to track which URLs we've already processed.
    """
    record_copy = record.copy()
    
    # DLT provides request context in the resource
    # We'll add URL tracking if available in the context
    # For now, we'll implement this when we integrate with the pipeline
    
    return record_copy


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


def _add_request_url(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Processing step to add the request URL for future deduplication.
    DLT doesn't provide request-level caching, so we implement it ourselves.
    """
    record_copy = record.copy()
    
    # Add URL metadata that can be used for request-level deduplication
    # Note: The actual URL needs to be passed through DLT context
    # For now, we'll add a placeholder that can be populated during processing
    record_copy["_baliza_request_url"] = None  # Will be set by pipeline context
    
    return record_copy


def create_modalidade_resources(
    endpoint_name: str, 
    start_date: str, 
    end_date: str, 
    modalidades: List[int]
) -> List[Dict[str, Any]]:
    """
    Create separate resources for each modalidade (for endpoints that require it).
    This handles endpoints like 'contratacoes_publicacao' that need modalidade parameter.
    """
    
    if endpoint_name not in ENDPOINT_CONFIG:
        raise ValueError(f"Unknown endpoint: {endpoint_name}")
        
    endpoint_config = ENDPOINT_CONFIG[endpoint_name]
    
    if not endpoint_config.requires_modalidade:
        raise ValueError(f"Endpoint {endpoint_name} does not require modalidade")
    
    resources = []
    
    for modalidade_id in modalidades:
        modalidade_enum = ModalidadeContratacao(modalidade_id)
        resource_name = f"{endpoint_name}_{modalidade_enum.name.lower()}"
        
        params = _build_endpoint_params(endpoint_config, start_date, end_date, [modalidade_id])
        
        resource = {
            "name": resource_name,
            "endpoint": {
                "path": endpoint_config.path,
                "method": "GET", 
                "params": params,
                "paginator": _get_paginator_config(endpoint_config),
                "data_selector": "data",
            },
            "primary_key": "_dlt_id",
            "write_disposition": "merge",
            "processing_steps": [
                {
                    "map": _add_hash_id
                },
                {
                    "map": _add_metadata
                }
            ]
        }
        
        resources.append(resource)
    
    return resources


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