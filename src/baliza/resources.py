"""
PNCP API Resource Definitions - Python Configuration

This module defines all PNCP API resources for the Baliza pipeline,
replacing the YAML configuration with type-safe Python definitions.

Key Architecture Decisions:
- Separate tables for /atualizacao endpoints (update history)
- Endpoint-specific page size limits based on API testing
- Logical separation between sync, backfill, and specialized resources
"""

from dataclasses import dataclass
from typing import List, Optional, Union, Dict, Any
from datetime import datetime
import calendar

from .enums import ModalidadeContratacao


@dataclass
class IncrementalConfig:
    """Configuration for incremental data extraction."""
    cursor_path: str
    initial_value: str


@dataclass
class ResourceConfig:
    """Typed configuration for a DLT resource."""
    name: str
    table_name: str
    endpoint_path: str
    page_size: int
    write_disposition: str
    primary_key: Union[str, List[str]]
    requires_modalidade: bool = False
    incremental_config: Optional[IncrementalConfig] = None
    additional_params: Optional[Dict[str, Any]] = None


# =============================================================================
# ENDPOINT-SPECIFIC PAGE SIZE LIMITS (from test script findings)
# =============================================================================

PAGE_SIZE_LIMITS = {
    # Modalidade-requiring endpoints (minimum 10, optimal tested values)
    "contratacoes_publicacao": 50,
    "contratacoes_atualizacao": 50,
    
    # Non-modalidade endpoints (based on PNCP API testing)
    "contratos": 100,  # Reduced from 500 for better reliability
    "contratos_atualizacao": 100,
    "atas": 100,  # Reduced from 500 for better reliability
    "atas_atualizacao": 100,
    
    # Specialized endpoints (minimum 10 required)
    "pca_usuario": 50,
    "instrumentos_cobranca": 50,
}


# =============================================================================
# SYNC RESOURCES (Incremental updates with separate history tables)
# =============================================================================

def create_sync_resources() -> List[ResourceConfig]:
    """
    Create resources for incremental synchronization.
    
    Uses /atualizacao endpoints that provide update history data.
    These are stored in separate tables from the main publication data
    to maintain proper audit trail and data lineage.
    """
    return [
        # Contratações - Update History (separate table for audit trail)
        ResourceConfig(
            name="contratacoes_atualizacao",
            table_name="contratacao_atualizacao",  # Separate table for updates
            endpoint_path="/v1/contratacoes/atualizacao",
            page_size=PAGE_SIZE_LIMITS["contratacoes_atualizacao"],
            write_disposition="merge",
            primary_key="numeroControlePNCP",
            requires_modalidade=True,  # Expands to 13 resources
            incremental_config=IncrementalConfig(
                cursor_path="dataAtualizacao",
                initial_value="20240101"
            ),
            additional_params={
                "dataInicial": "20240101",  # Required by API
                "dataFinal": "20241231",    # Will be updated by incremental
            }
        ),
        
        # Contratos - Update History (separate table for audit trail)
        ResourceConfig(
            name="contratos_atualizacao", 
            table_name="contrato_atualizacao",  # Separate table for updates
            endpoint_path="/v1/contratos/atualizacao",
            page_size=PAGE_SIZE_LIMITS["contratos_atualizacao"],
            write_disposition="merge",
            primary_key="numeroControlePNCP",
            requires_modalidade=False,  # Single resource
            incremental_config=IncrementalConfig(
                cursor_path="dataAtualizacao",
                initial_value="20240101"
            ),
            additional_params={
                "dataInicial": "20240101",  # Required by API
                "dataFinal": "20241231",    # Will be updated by incremental
            }
        ),
        
        # Atas - Update History (separate table for audit trail)
        ResourceConfig(
            name="atas_atualizacao",
            table_name="ata_registro_preco_atualizacao",  # Separate table for updates
            endpoint_path="/v1/atas/atualizacao", 
            page_size=PAGE_SIZE_LIMITS["atas_atualizacao"],
            write_disposition="merge",
            primary_key="numeroControlePNCPAta",
            requires_modalidade=False,  # Single resource
            incremental_config=IncrementalConfig(
                cursor_path="dataAtualizacao",
                initial_value="20240101"
            ),
            additional_params={
                "dataInicial": "20240101",  # Required by API
                "dataFinal": "20241231",    # Will be updated by incremental
            }
        ),
    ]


# =============================================================================
# BACKFILL RESOURCES (Historical publication data in main tables)
# =============================================================================

def create_backfill_resources() -> List[ResourceConfig]:
    """
    Create resources for historical data backfill.
    
    Uses main endpoints that contain original publication data.
    These are stored in the primary tables and used for historical analysis.
    """
    return [
        # Contratações - Original Publications (main table)
        ResourceConfig(
            name="contratacoes_publicacao",
            table_name="contratacao_publicacao",  # Main table for publications
            endpoint_path="/v1/contratacoes/publicacao",
            page_size=PAGE_SIZE_LIMITS["contratacoes_publicacao"],
            write_disposition="replace",
            primary_key="numeroControlePNCP",
            requires_modalidade=True,  # Expands to 13 resources
            additional_params={
                "dataInicial": "20240101",  # Required by API - will be updated by backfill
                "dataFinal": "20241231",    # Required by API - will be updated by backfill
            }
        ),
        
        # Contratos - Main Data (main table)
        ResourceConfig(
            name="contratos",
            table_name="contrato",  # Main table for contracts
            endpoint_path="/v1/contratos",
            page_size=PAGE_SIZE_LIMITS["contratos"],
            write_disposition="replace", 
            primary_key="numeroControlePNCP",
            requires_modalidade=False,  # Single resource
            additional_params={
                "dataInicial": "20240101",  # Required by API - will be updated by backfill
                "dataFinal": "20241231",    # Required by API - will be updated by backfill
            }
        ),
        
        # Atas - Main Data (main table)
        ResourceConfig(
            name="atas",
            table_name="ata_registro_preco",  # Main table for atas
            endpoint_path="/v1/atas",
            page_size=PAGE_SIZE_LIMITS["atas"],
            write_disposition="replace",
            primary_key="numeroControlePNCPAta", 
            requires_modalidade=False,  # Single resource
            additional_params={
                "dataInicial": "20240101",  # Required by API - will be updated by backfill
                "dataFinal": "20241231",    # Required by API - will be updated by backfill
            }
        ),
    ]


# =============================================================================
# SPECIALIZED RESOURCES (PCA and other specialized endpoints)
# =============================================================================

def create_specialized_resources() -> List[ResourceConfig]:
    """
    Create resources for specialized endpoints (PCA, instrumentos, etc.).
    
    These endpoints have unique parameter requirements and data structures.
    """
    return [
        # PCA Usuario
        ResourceConfig(
            name="pca_usuario",
            table_name="plano_contratacao",
            endpoint_path="/v1/pca/usuario",
            page_size=PAGE_SIZE_LIMITS["pca_usuario"],
            write_disposition="replace",
            primary_key="idPcaPncp",
            requires_modalidade=False,
            additional_params={
                "anoPca": datetime.now().year,  # Current year by default
                "idUsuario": 1,
            }
        ),
        
        # Instrumentos de Cobrança
        ResourceConfig(
            name="instrumentos_cobranca",
            table_name="instrumento_cobranca", 
            endpoint_path="/v1/instrumentoscobranca/inclusao",
            page_size=PAGE_SIZE_LIMITS["instrumentos_cobranca"],
            write_disposition="merge",
            primary_key=["cnpj", "ano", "sequencialContrato", "sequencialInstrumentoCobranca"],
            requires_modalidade=False,
            incremental_config=IncrementalConfig(
                cursor_path="dataInclusao",
                initial_value="20240101"
            )
        ),
    ]


# =============================================================================
# MODERNIZED DLT REST API CONFIGURATION (DLT Best Practices)
# =============================================================================

def create_pncp_rest_config(resource_type: str, base_url: str, year_month: Optional[str] = None) -> Dict[str, Any]:
    """
    Create modernized RESTAPIConfig using DLT best practices.
    
    This replaces the custom abstraction layer with direct DLT configuration,
    using monthly extraction instead of sync.
    
    Args:
        resource_type: 'monthly', 'backfill', or 'specialized'
        base_url: PNCP API base URL
        year_month: Year and month for extraction (YYYYMM format)
        
    Returns:
        RESTAPIConfig dictionary ready for rest_api_source()
    """
    
    # Base client configuration
    config = {
        "client": {
            "base_url": base_url,
            "paginator": {
                "type": "page_number",
                "page_param": "pagina", 
                "total_path": "totalPaginas",
                "base_page": 1,
            },
        },
        "resource_defaults": {
            "write_disposition": "merge",
            "max_table_nesting": 2,
        },
        "resources": []
    }
    
    # Add resources based on type
    if resource_type == "monthly":
        config["resources"].extend(_create_monthly_rest_resources(year_month))
    elif resource_type == "backfill":
        config["resources"].extend(_create_backfill_rest_resources())
    elif resource_type == "specialized":
        config["resources"].extend(_create_specialized_rest_resources())
    else:
        raise ValueError(f"Unknown resource type: {resource_type}")
    
    return config


def _get_month_date_range(year_month: str) -> tuple[str, str]:
    """
    Converte YYYYMM para range de datas do mês no formato YYYYMMDD.
    
    Args:
        year_month: Ano e mês no formato YYYYMM
        
    Returns:
        Tupla (data_inicial, data_final) no formato YYYYMMDD
    """
    if not year_month:
        raise ValueError("year_month é obrigatório para extração mensal")
        
    year = int(year_month[:4])
    month = int(year_month[4:])
    
    # Primeiro dia do mês
    first_day = f"{year:04d}{month:02d}01"
    
    # Último dia do mês
    last_day_of_month = calendar.monthrange(year, month)[1]
    last_day = f"{year:04d}{month:02d}{last_day_of_month:02d}"
    
    return first_day, last_day


def _create_monthly_rest_resources(year_month: Optional[str]) -> List[Dict[str, Any]]:
    """Create monthly resources using modern DLT REST API format."""
    if not year_month:
        raise ValueError("year_month é obrigatório para extração mensal")
    
    data_inicial, data_final = _get_month_date_range(year_month)
    resources = []
    
    # For testing, start with just modalidade 1 to ensure it works
    # Later we can expand to all modalidades: [1, 3, 4, 5, 6]
    common_modalidades = [1]
    
    # Contratações Publicação (with common modalidades only) - dados do mês
    for modalidade_value in common_modalidades:
        resources.append({
            "name": f"contratacoes_publicacao_mod{modalidade_value}",
            "table_name": "contratacao_publicacao",
            "endpoint": {
                "path": "/v1/contratacoes/publicacao",
                "data_selector": "data",
                "params": {
                    "pagina": 1,
                    "tamanhoPagina": PAGE_SIZE_LIMITS["contratacoes_publicacao"],
                    "codigoModalidadeContratacao": int(modalidade_value),
                    "dataInicial": data_inicial,
                    "dataFinal": data_final,
                }
            },
            "write_disposition": "replace",
            "primary_key": "numeroControlePNCP",
        })
    
    # Contratos (single resource) - dados do mês
    resources.append({
        "name": "contratos",
        "table_name": "contrato", 
        "endpoint": {
            "path": "/v1/contratos",
            "data_selector": "data",
            "params": {
                "pagina": 1,
                "tamanhoPagina": PAGE_SIZE_LIMITS["contratos"],
                "dataInicial": data_inicial,
                "dataFinal": data_final,
            }
        },
        "write_disposition": "replace",
        "primary_key": "numeroControlePNCP",
    })
    
    # Atas (single resource) - dados do mês
    resources.append({
        "name": "atas",
        "table_name": "ata_registro_preco",
        "endpoint": {
            "path": "/v1/atas",
            "data_selector": "data", 
            "params": {
                "pagina": 1,
                "tamanhoPagina": PAGE_SIZE_LIMITS["atas"],
                "dataInicial": data_inicial,
                "dataFinal": data_final,
            }
        },
        "write_disposition": "replace", 
        "primary_key": "numeroControlePNCPAta",
    })
    
    return resources


def _create_backfill_rest_resources() -> List[Dict[str, Any]]:
    """Create backfill resources using modern DLT REST API format."""
    resources = []
    
    # Contratações Publicação (with modalidade expansion) 
    for modalidade in ModalidadeContratacao:
        resources.append({
            "name": f"contratacoes_publicacao_mod{modalidade.value}",
            "table_name": "contratacao_publicacao",
            "endpoint": {
                "path": "/v1/contratacoes/publicacao",
                "data_selector": "data",
                "params": {
                    "pagina": 1,
                    "tamanhoPagina": PAGE_SIZE_LIMITS["contratacoes_publicacao"],
                    "codigoModalidadeContratacao": int(modalidade.value),
                    # Note: backfill will update these dates dynamically
                    "dataInicial": "20240101",
                    "dataFinal": "20241231",
                }
            },
            "write_disposition": "replace",
            "primary_key": "numeroControlePNCP",
        })
    
    # Contratos (single resource)
    resources.append({
        "name": "contratos",
        "table_name": "contrato",
        "endpoint": {
            "path": "/v1/contratos", 
            "data_selector": "data",
            "params": {
                "pagina": 1,
                "tamanhoPagina": PAGE_SIZE_LIMITS["contratos"],
                "dataInicial": "20240101",
                "dataFinal": "20241231",
            }
        },
        "write_disposition": "replace",
        "primary_key": "numeroControlePNCP",
    })
    
    # Atas (single resource)
    resources.append({
        "name": "atas",
        "table_name": "ata_registro_preco",
        "endpoint": {
            "path": "/v1/atas",
            "data_selector": "data",
            "params": {
                "pagina": 1,
                "tamanhoPagina": PAGE_SIZE_LIMITS["atas"],
                "dataInicial": "20240101", 
                "dataFinal": "20241231",
            }
        },
        "write_disposition": "replace",
        "primary_key": "numeroControlePNCPAta",
    })
    
    return resources


def _create_specialized_rest_resources() -> List[Dict[str, Any]]:
    """Create specialized resources using modern DLT REST API format."""
    resources = []
    
    # PCA Usuario
    resources.append({
        "name": "pca_usuario",
        "table_name": "plano_contratacao",
        "endpoint": {
            "path": "/v1/pca/usuario",
            "data_selector": "data",
            "params": {
                "pagina": 1, 
                "tamanhoPagina": PAGE_SIZE_LIMITS["pca_usuario"],
                "anoPca": datetime.now().year,
                "idUsuario": 1,
            }
        },
        "write_disposition": "replace",
        "primary_key": "idPcaPncp",
    })
    
    # Instrumentos de Cobrança
    resources.append({
        "name": "instrumentos_cobranca",
        "table_name": "instrumento_cobranca",
        "endpoint": {
            "path": "/v1/instrumentoscobranca/inclusao",
            "data_selector": "data",
            "params": {
                "pagina": 1,
                "tamanhoPagina": PAGE_SIZE_LIMITS["instrumentos_cobranca"],
                "dataInicial": "{incremental.last_value}",
                "dataFinal": "20241231",
            },
            "incremental": {
                "cursor_path": "dataInclusao",
                "initial_value": "20240101",
            }
        },
        "write_disposition": "merge",
        "primary_key": ["cnpj", "ano", "sequencialContrato", "sequencialInstrumentoCobranca"],
    })
    
    return resources


# =============================================================================
# LEGACY RESOURCE FACTORY (for backward compatibility)
# =============================================================================

def create_monthly_resources() -> List[ResourceConfig]:
    """
    Create resources for monthly data extraction.
    
    Similar to backfill but designed for single-month extraction.
    """
    return [
        # Contratações - Main Publications (main table)
        ResourceConfig(
            name="contratacoes_publicacao",
            table_name="contratacao_publicacao",  # Main table for publications
            endpoint_path="/v1/contratacoes/publicacao",
            page_size=PAGE_SIZE_LIMITS["contratacoes_publicacao"],
            write_disposition="replace",
            primary_key="numeroControlePNCP",
            requires_modalidade=True,  # Expands to 13 resources
        ),
        
        # Contratos - Main Data (main table)
        ResourceConfig(
            name="contratos",
            table_name="contrato",  # Main table for contracts
            endpoint_path="/v1/contratos",
            page_size=PAGE_SIZE_LIMITS["contratos"],
            write_disposition="replace", 
            primary_key="numeroControlePNCP",
            requires_modalidade=False,  # Single resource
        ),
        
        # Atas - Main Data (main table)
        ResourceConfig(
            name="atas",
            table_name="ata_registro_preco",  # Main table for atas
            endpoint_path="/v1/atas",
            page_size=PAGE_SIZE_LIMITS["atas"],
            write_disposition="replace",
            primary_key="numeroControlePNCPAta", 
            requires_modalidade=False,  # Single resource
        ),
    ]


def get_resources_by_type(resource_type: str) -> List[ResourceConfig]:
    """
    Get resources by type with proper type safety.
    
    Args:
        resource_type: 'monthly', 'sync', 'backfill', or 'specialized'
        
    Returns:
        List of configured resources for the specified type
    """
    resource_factories = {
        "monthly": create_monthly_resources,
        "sync": create_sync_resources,
        "backfill": create_backfill_resources, 
        "specialized": create_specialized_resources,
    }
    
    factory = resource_factories.get(resource_type)
    if not factory:
        raise ValueError(f"Unknown resource type: {resource_type}. Must be one of: {list(resource_factories.keys())}")
    
    return factory()


def expand_modalidade_resources(resource_config: ResourceConfig) -> List[Dict[str, Any]]:
    """
    Expand a resource that requires modalidade into 13 separate DLT resources.
    
    Args:
        resource_config: Base resource configuration
        
    Returns:
        List of 13 DLT resource configurations (one per modalidade)
    """
    if not resource_config.requires_modalidade:
        raise ValueError(f"Resource {resource_config.name} does not require modalidade expansion")
    
    resources = []
    
    for modalidade in ModalidadeContratacao:
        # Create DLT resource configuration
        dlt_config = {
            "name": f"{resource_config.name}_mod{modalidade.value}",
            "endpoint": {
                "path": resource_config.endpoint_path,
                "data_selector": "data",
                "params": {
                    "pagina": 1,
                    "tamanhoPagina": resource_config.page_size,
                    "codigoModalidadeContratacao": modalidade.value,
                    **(resource_config.additional_params or {})
                }
            },
            "write_disposition": resource_config.write_disposition,
            "primary_key": resource_config.primary_key,
        }
        
        # Add incremental configuration to endpoint params (DLT 1.14+ format)
        if resource_config.incremental_config:
            dlt_config["endpoint"]["incremental"] = {
                "cursor_path": resource_config.incremental_config.cursor_path,
                "initial_value": resource_config.incremental_config.initial_value,
            }
        
        resources.append(dlt_config)
    
    return resources


def create_dlt_resource(resource_config: ResourceConfig) -> Dict[str, Any]:
    """
    Convert a ResourceConfig to DLT resource configuration format.
    
    Args:
        resource_config: Typed resource configuration
        
    Returns:
        DLT-compatible resource configuration
    """
    dlt_config = {
        "name": resource_config.name,
        "endpoint": {
            "path": resource_config.endpoint_path,
            "data_selector": "data", 
            "params": {
                "pagina": 1,
                "tamanhoPagina": resource_config.page_size,
                **(resource_config.additional_params or {})
            }
        },
        "write_disposition": resource_config.write_disposition,
        "primary_key": resource_config.primary_key,
    }
    
    # Add incremental configuration to endpoint (DLT 1.14+ format)
    if resource_config.incremental_config:
        dlt_config["endpoint"]["incremental"] = {
            "cursor_path": resource_config.incremental_config.cursor_path,
            "initial_value": resource_config.incremental_config.initial_value,
        }
    
    return dlt_config


def prepare_resources_for_dlt(resource_type: str) -> List[Dict[str, Any]]:
    """
    Prepare final DLT resources with modalidade expansion.
    
    Args:
        resource_type: 'sync', 'backfill', or 'specialized'
        
    Returns:
        List of DLT-ready resource configurations
    """
    resource_configs = get_resources_by_type(resource_type)
    final_resources = []
    
    for config in resource_configs:
        if config.requires_modalidade:
            # Expand to 13 modalidade resources
            final_resources.extend(expand_modalidade_resources(config))
        else:
            # Single resource
            final_resources.append(create_dlt_resource(config))
    
    return final_resources


# =============================================================================
# SUMMARY INFORMATION
# =============================================================================

def get_resource_summary() -> Dict[str, Dict[str, Any]]:
    """
    Get summary information about all configured resources.
    
    Returns:
        Dictionary with resource type breakdown and counts
    """
    summary = {}
    
    for resource_type in ["monthly", "backfill", "specialized"]:
        configs = get_resources_by_type(resource_type)
        total_final = sum(13 if config.requires_modalidade else 1 for config in configs)
        
        summary[resource_type] = {
            "base_resources": len(configs),
            "final_resources": total_final,
            "resources": [
                {
                    "name": config.name,
                    "table": config.table_name,
                    "requires_modalidade": config.requires_modalidade,
                    "page_size": config.page_size,
                    "has_incremental": config.incremental_config is not None,
                }
                for config in configs
            ]
        }
    
    return summary