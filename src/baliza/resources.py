# src/baliza/resources.py

"""
PNCP API Resource Definitions - Modernized for dlt Best Practices

This module defines all PNCP API resources using the modern, declarative
RESTAPIConfig pattern. It replaces the custom abstraction layer with direct
dlt configuration, using modern incremental loading with placeholder syntax.
"""

from typing import Dict, Any, List
from datetime import datetime, timedelta

from .enums import ModalidadeContratacao

# Page size limits discovered from API testing
PAGE_SIZE_LIMITS = {
    "contratacoes": 50,
    "contratos": 500,
    "atas": 500,
    "pca_usuario": 50,
    "instrumentos_cobranca": 50,
}

def _create_backfill_resources() -> List[Dict[str, Any]]:
    """Creates resource definitions for historical backfill."""
    resources = []
    # Contratações (with modalidade expansion)
    for modalidade in ModalidadeContratacao:
        resources.append({
            "name": f"contratacoes_publicacao_mod{modalidade.value}",
            "table_name": "contratacoes_publicacao",
            "endpoint": {
                "path": "/v1/contratacoes/publicacao",
                "params": {
                    "codigoModalidadeContratacao": modalidade.value,
                    # Placeholders will be filled by the backfill resource
                    "dataInicial": "{start_chunk}",
                    "dataFinal": "{end_chunk}",
                },
            },
        })
    # Contratos
    resources.append({
        "name": "contratos_publicacao",
        "table_name": "contratos_publicacao",
        "endpoint": {
            "path": "/v1/contratos",
            "params": {
                "dataInicial": "{start_chunk}",
                "dataFinal": "{end_chunk}",
            },
        },
    })
    # Atas
    resources.append({
        "name": "atas_publicacao",
        "table_name": "atas_publicacao",
        "primary_key": "numeroControlePNCPAta",
        "endpoint": {
            "path": "/v1/atas",
            "params": {
                "dataInicial": "{start_chunk}",
                "dataFinal": "{end_chunk}",
            },
        },
    })
    return resources

def create_pncp_rest_config(resource_type: str, base_url: str) -> Dict[str, Any]:
    """
    Creates the full, declarative RESTAPIConfig for the PNCP source.
    """
    if resource_type == "backfill":
        resource_list = _create_backfill_resources()
    else:
        raise ValueError(f"Unknown resource type: {resource_type}")

    # Set page size for each resource based on its name
    for resource in resource_list:
        if "contratacoes" in resource["name"]:
            resource["endpoint"]["params"]["tamanhoPagina"] = PAGE_SIZE_LIMITS["contratacoes"]
        elif "contratos" in resource["name"]:
            resource["endpoint"]["params"]["tamanhoPagina"] = PAGE_SIZE_LIMITS["contratos"]
        elif "atas" in resource["name"]:
            resource["endpoint"]["params"]["tamanhoPagina"] = PAGE_SIZE_LIMITS["atas"]

    config = {
        "client": {
            "base_url": base_url,
            "paginator": {
                "type": "page_number",
                "page_param": "pagina",
                "total_path": "totalPaginas",
                "maximum_page": 2000, # Safety break for PNCP API
                "base_page": 1,
            },
        },
        "resource_defaults": {
            "primary_key": "numeroControlePNCP",
            "write_disposition": "merge",
            "endpoint": {
                "data_selector": "data",
            },
        },
        "resources": resource_list,
    }
    return config
