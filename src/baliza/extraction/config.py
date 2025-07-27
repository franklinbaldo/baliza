"""
PNCP REST API Configuration for DLT
Converts legacy ENDPOINT_CONFIG to dlt REST API format
"""

from datetime import date, timedelta
from typing import Dict, Any, List
from baliza.settings import ENDPOINT_CONFIG, settings
from baliza.schemas import ModalidadeContratacao, ClassificacaoSuperior, UsuarioSistema, get_anos_pca_disponiveis
from baliza.utils import hash_sha256
from baliza.models import (
    PaginaRetornoRecuperarCompraPublicacaoDTO,
    PaginaRetornoRecuperarContratoDTO, 
    PaginaRetornoAtaRegistroPrecoPeriodoDTO,
    PaginaRetornoConsultarInstrumentoCobrancaDTO,
    PaginaRetornoPlanoContratacaoComItensDoUsuarioDTO
)

# Parameter variations using enums - much cleaner and type-safe!
# Modalidades that have data (excluding modalidade 2 which returned no results)
MODALIDADES_WITH_DATA = [
    ModalidadeContratacao.LEILAO_ELETRONICO,
    ModalidadeContratacao.CONCURSO, 
    ModalidadeContratacao.CONCORRENCIA_ELETRONICA,
    ModalidadeContratacao.CONCORRENCIA_PRESENCIAL,
    ModalidadeContratacao.PREGAO_ELETRONICO,
    ModalidadeContratacao.PREGAO_PRESENCIAL,
    ModalidadeContratacao.DISPENSA_DE_LICITACAO,
    ModalidadeContratacao.INEXIGIBILIDADE,
    ModalidadeContratacao.MANIFESTACAO_DE_INTERESSE,
    ModalidadeContratacao.PRE_QUALIFICACAO,
    ModalidadeContratacao.CREDENCIAMENTO,
    ModalidadeContratacao.LEILAO_PRESENCIAL,
]

# All classification codes that contain data
CLASSIFICATION_CODES_WITH_DATA = [code.value for code in ClassificacaoSuperior]

# All years since PNCP launch (dynamic function for future years)
YEARS_WITH_DATA = get_anos_pca_disponiveis()

# All known user system IDs 
USER_IDS_WITH_DATA = [user.value for user in UsuarioSistema]


def create_pncp_rest_config(
    start_date: str = None, end_date: str = None, modalidades: List[int] = None
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
            "Accept": "application/json",
        },
        # Note: timeout would need to be configured through session if needed
        # Note: DLT doesn't provide request-level caching, so we implement deduplication at data level
    }

    # Endpoint to Pydantic model mapping for schema communication
    endpoint_models = {
        "contratacoes_publicacao": PaginaRetornoRecuperarCompraPublicacaoDTO,
        "contratos": PaginaRetornoRecuperarContratoDTO,
        "atas": PaginaRetornoAtaRegistroPrecoPeriodoDTO,
        "contratacoes_atualizacao": PaginaRetornoRecuperarCompraPublicacaoDTO,
        "contratos_atualizacao": PaginaRetornoRecuperarContratoDTO,
        "atas_atualizacao": PaginaRetornoAtaRegistroPrecoPeriodoDTO,
        "instrumentoscobranca_inclusao": PaginaRetornoConsultarInstrumentoCobrancaDTO,
        "pca": PaginaRetornoPlanoContratacaoComItensDoUsuarioDTO,
        "pca_usuario": PaginaRetornoPlanoContratacaoComItensDoUsuarioDTO,
        "pca_atualizacao": PaginaRetornoPlanoContratacaoComItensDoUsuarioDTO,
        # Note: contratacoes_proposta and contratacao_especifica endpoints don't have specific models yet
    }

    # Build resources from ENDPOINT_CONFIG
    resources = []

    for endpoint_name, endpoint_config in ENDPOINT_CONFIG.items():
        # Process ALL endpoints - no phase restrictions

        # Get appropriate page size for this endpoint
        page_size = settings.ENDPOINT_PAGE_LIMITS.get(
            endpoint_name, settings.default_page_size
        )

        # Generate parameter variations for endpoints that support them
        parameter_variations = _get_parameter_variations(endpoint_name, endpoint_config, modalidades)
        
        # Create a resource for each parameter variation
        for variation in parameter_variations:
            variation_name = variation["name"]
            variation_params = variation["params"]
            
            # Get Pydantic model for this variation based on base endpoint name
            # For variations like "contratos_mod1", use the base endpoint name "contratos"
            base_endpoint = variation_name.split('_mod')[0].split('_20')[0]
            pydantic_model = endpoint_models.get(base_endpoint)
            
            # Base resource configuration
            resource = {
                "name": variation_name,
                "endpoint": {
                    "path": endpoint_config.path,
                    "method": "GET",
                    "params": _build_endpoint_params_with_variation(
                        endpoint_config, start_date, end_date, variation_params, page_size
                    ),
                    "paginator": _get_paginator_config(endpoint_config),
                    "data_selector": "data",  # PNCP responses have data array
                    "response_actions": [
                        {
                            "status_code": 204,
                            "action": "ignore"  # 204 No Content: valid response with no data
                        },
                        {
                            "status_code": 404,
                            "action": "ignore"  # 404 Not Found: no data for this parameter combination (normal)
                        }
                    ]
                },
                "write_disposition": "replace",  # Force replace to avoid merge strategy warnings
                "schema_contract": "evolve",  # Use simple evolve mode for maximum flexibility
                "processing_steps": [
                    {
                        "map": _add_hash_id  # Map function for deduplication
                    },
                    {
                        "map": _add_metadata  # Map function for metadata
                    },
                ],
                # Note: Incremental loading handled by gap detection instead of DLT incremental
            }

            # Use hybrid approach: Pydantic models for endpoints that work, permissive for others
            # Based on validation testing, some endpoints have accurate models, others don't
            
            endpoints_with_working_models = ["atas", "atas_atualizacao"]
            
            if base_endpoint in endpoints_with_working_models and pydantic_model:
                # Use Pydantic model for endpoints we've validated
                resource["columns"] = pydantic_model
            else:
                # Use permissive schema for endpoints with incomplete/incorrect Pydantic models
                resource["columns"] = {
                    "_dlt_id": {"data_type": "text", "nullable": False},
                    "_baliza_extracted_at": {"data_type": "date", "nullable": False},
                    # Common optional fields from API responses that frequently cause warnings
                    "unidade_sub_rogada": {"data_type": "text", "nullable": True},
                    "orgao_sub_rogado": {"data_type": "text", "nullable": True},
                    "ni_fornecedor_sub_contratado": {"data_type": "text", "nullable": True},
                    "nome_fornecedor_sub_contratado": {"data_type": "text", "nullable": True},
                    "tipo_pessoa_sub_contratada": {"data_type": "text", "nullable": True},
                    "identificador_cipi": {"data_type": "text", "nullable": True},
                    "url_cipi": {"data_type": "text", "nullable": True},
                    "data_cancelamento": {"data_type": "text", "nullable": True},
                    "cnpj_orgao_subrogado": {"data_type": "text", "nullable": True},
                    "nome_orgao_subrogado": {"data_type": "text", "nullable": True},
                    "codigo_unidade_orgao_subrogado": {"data_type": "text", "nullable": True},
                    "nome_unidade_orgao_subrogado": {"data_type": "text", "nullable": True},
                }
            # Note: Additional fields will be allowed due to schema_contract: "evolve"

            resources.append(resource)

    return {"client": client_config, "resources": resources}


def _get_parameter_variations(endpoint_name: str, endpoint_config, modalidades: List[int] = None) -> List[Dict[str, Any]]:
    """
    Generate parameter variations for endpoints that support multiple values.
    
    Returns list of variations, each with 'name' and 'params' keys.
    """
    variations = []
    
    # Handle endpoints with modalidade variations
    if endpoint_config.requires_modalidade:
        # Use all modalidades with data for comprehensive extraction
        target_modalidades = MODALIDADES_WITH_DATA
        
        for modalidade in target_modalidades:
            variation_name = f"{endpoint_name}_mod{modalidade.value}"
            variation_params = {"codigoModalidadeContratacao": modalidade.value}
            variations.append({
                "name": variation_name,
                "params": variation_params
            })
    
    # Handle PCA endpoints with classification code variations
    elif "pca/" in endpoint_config.path:
        for year in YEARS_WITH_DATA:
            for classification_code in CLASSIFICATION_CODES_WITH_DATA:
                variation_name = f"{endpoint_name}_{year}_class{classification_code}"
                variation_params = {
                    "anoPca": year,
                    "codigoClassificacaoSuperior": classification_code
                }
                variations.append({
                    "name": variation_name,
                    "params": variation_params
                })
    
    # Handle PCA usuario endpoint with user ID variations
    elif "pca/usuario" in endpoint_config.path:
        for year in YEARS_WITH_DATA:
            for user_id in USER_IDS_WITH_DATA:
                variation_name = f"{endpoint_name}_{year}_user{user_id}"
                variation_params = {
                    "anoPca": year,
                    "idUsuario": user_id
                }
                variations.append({
                    "name": variation_name,
                    "params": variation_params
                })
    
    # Handle contratacoes_proposta with optional modalidade variations
    elif "proposta" in endpoint_config.path:
        # Base variation without modalidade
        variations.append({
            "name": endpoint_name,
            "params": {}
        })
        
        # Additional variations with modalidades that have data
        for modalidade in MODALIDADES_WITH_DATA:
            variation_name = f"{endpoint_name}_mod{modalidade.value}"
            variation_params = {"codigoModalidadeContratacao": modalidade.value}
            variations.append({
                "name": variation_name,
                "params": variation_params
            })
    
    # Default: single variation for endpoints without parameter variations
    else:
        variations.append({
            "name": endpoint_name,
            "params": {}
        })
    
    return variations


def _build_endpoint_params_with_variation(
    endpoint_config,
    start_date: str,
    end_date: str,
    variation_params: Dict[str, Any],
    page_size: int = None,
) -> Dict[str, Any]:
    """Build parameters for an endpoint including parameter variations."""
    
    # Start with base parameters
    params = _build_endpoint_params(endpoint_config, start_date, end_date, [], page_size)
    
    # Override/add variation-specific parameters
    params.update(variation_params)
    
    return params


def _build_endpoint_params(
    endpoint_config,
    start_date: str,
    end_date: str,
    modalidades: List[int],
    page_size: int = None,
) -> Dict[str, Any]:
    """Build parameters for an endpoint based on its configuration."""

    # Use provided page_size or fallback to endpoint default
    effective_page_size = (
        page_size if page_size is not None else endpoint_config.default_page_size
    )

    params = {
        "tamanhoPagina": effective_page_size,
        "pagina": 1,  # Will be handled by paginator
    }

    # Special handling for specific endpoints based on API requirements
    endpoint_path = endpoint_config.path
    
    # Handle contratacoes/proposta - requires dataFinal >= current date
    if "proposta" in endpoint_path:
        # Use a future date for proposta endpoint (needs dataFinal >= today)
        from datetime import date, timedelta
        future_date = (date.today() + timedelta(days=365*5)).strftime("%Y%m%d")  # 5 years in future
        params["dataFinal"] = future_date
        # Don't add dataInicial for proposta endpoint
        
    # Handle instrumentoscobranca/inclusao - needs recent dates within 30-day window
    elif "instrumentoscobranca" in endpoint_path:
        from datetime import date, timedelta
        today = date.today()
        thirty_days_ago = today - timedelta(days=30)
        params["dataInicial"] = thirty_days_ago.strftime("%Y%m%d")
        params["dataFinal"] = today.strftime("%Y%m%d")
        
    # Handle PCA endpoints - need special parameters
    elif "pca/" in endpoint_path:
        from datetime import date
        current_year = date.today().year
        params["anoPca"] = current_year
        params["codigoClassificacaoSuperior"] = "00"  # Default classification code
        # Don't add date parameters for base pca endpoint
        
    elif "pca/usuario" in endpoint_path:
        from datetime import date
        current_year = date.today().year
        params["anoPca"] = current_year
        params["idUsuario"] = 1  # Default user ID
        # Don't add date parameters for pca/usuario endpoint
        
    elif "pca/atualizacao" in endpoint_path:
        from datetime import date, timedelta
        today = date.today()
        thirty_days_ago = today - timedelta(days=30)
        params["dataInicio"] = thirty_days_ago.strftime("%Y%m%d")
        params["dataFim"] = today.strftime("%Y%m%d")
        
    else:
        # Standard date parameter handling for other endpoints
        if "dataInicial" in endpoint_config.required_params:
            params["dataInicial"] = start_date
        if "dataFinal" in endpoint_config.required_params:
            params["dataFinal"] = end_date

    # Add modalidade if required and provided
    if endpoint_config.requires_modalidade and modalidades:
        # Note: This function handles single modalidade per request by design.
        # Multiple modalidades are handled by the gap detector creating separate gaps,
        # each with a single modalidade, implementing the month × modalidade matrix pattern.
        params["codigoModalidadeContratacao"] = modalidades[0]
    
    return params


def _get_paginator_config(endpoint_config) -> Dict[str, Any]:
    """Get appropriate paginator configuration for endpoint."""

    # PNCP uses page number pagination with custom field names
    return {
        "type": "page_number",
        "page_param": "pagina",  # PNCP parameter name
        "total_path": "totalPaginas",  # PNCP field for total pages
        "base_page": 1,  # PNCP starts at page 1
        "stop_after_empty_page": True,  # Stop if no data returned
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
