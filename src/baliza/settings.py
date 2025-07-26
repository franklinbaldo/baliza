from typing import Dict, List, ClassVar

from pydantic import BaseModel
from pydantic_settings import BaseSettings



class PNCPAPISettings(BaseSettings):
    """API settings for PNCP."""

    # TODO: This class was previously identified as potentially redundant and has
    #       since been removed or consolidated into the main `Settings` class.
    #       Ensure that all its previous functionalities and configurations are
    #       properly handled within the `Settings` class or elsewhere.
    pass


class Settings(BaseSettings):
    """General application settings."""

    # API Configuration
    pncp_api_base_url: str = "https://pncp.gov.br/api/consulta"

    # Database Configuration
    database_path: str = "data/baliza.duckdb"
    temp_directory: str = "data/tmp"
    duckdb_threads: int = 8
    duckdb_memory_limit: str = "4GB"
    duckdb_enable_progress_bar: bool = True

    # Rate Limiting
    requests_per_minute: int = 120
    requests_per_hour: int = 7200
    concurrent_endpoints: int = 12

    # Retry Configuration
    max_retry_attempts: int = 3
    retry_backoff_factor: float = 2.0
    retry_backoff_max: int = 300

    # Circuit Breaker
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: int = 300

    # Default Date Ranges
    default_date_range_days: int = 7
    max_date_range_days: int = 30

    # All 12 PNCP Endpoints (no phases)
    all_pncp_endpoints: List[str] = [
        "contratacoes_publicacao",
        "contratacoes_atualizacao", 
        "contratacoes_proposta",
        "contratos",
        "contratos_atualizacao",
        "atas",
        "atas_atualizacao",
        "instrumentoscobranca_inclusao",
        "pca",
        "pca_usuario",
        "pca_atualizacao",
        "contratacao_especifica",
    ]

    # Pagination
    default_page_size: int = 500  # Most endpoints support 500
    # TODO: Evaluate if MAX_PAGE_SIZE is still necessary. The `ENDPOINT_PAGE_LIMITS`
    #       dictionary seems to define specific page size limits per endpoint,
    #       potentially making this global constant redundant. If it's only used
    #       as a general guideline, consider removing it to avoid confusion.
    MAX_PAGE_SIZE: int = 500
    
    # Specific page size limits per endpoint (from endpoint_extraction_strategy.md)
    ENDPOINT_PAGE_LIMITS: ClassVar[Dict[str, int]] = {
        "contratacoes_publicacao": 50,   # Max 50 (required modalidade param)
        "contratos": 500,                # Max 500
        "atas": 500,                     # Max 500
        "instrumentos_cobranca": 100,    # Max 100 (if we add this endpoint later)
        # Default 500 for other endpoints
    }

    # Schema Validation
    VALIDATE_SCHEMA: bool = True
    SCHEMA_FINGERPRINT_CHECK: bool = True

    # Data Retention
    retention_days_raw: int = 365
    retention_days_staging: int = 180
    retention_days_logs: int = 90

    # Security
    secret_key: str = "dev-key-change-in-production"  # Override via BALIZA_SECRET_KEY env var
    enable_authentication: bool = False

    # Monitoring
    cpu_alert_threshold: float = 90.0  # Percentage
    memory_alert_threshold: float = 90.0  # Percentage

    # Logging
    log_level: str = "INFO"

    class Config:
        env_file = ".env"


class PageSizeLimits(BaseModel):
    """Pydantic model for page size limits."""

    min: int
    max: int


class EndpointConfig(BaseModel):
    """Pydantic model for endpoint configuration."""

    path: str
    required_params: List[str]
    optional_params: List[str]
    page_size_limits: PageSizeLimits
    default_page_size: int
    priority: int
    requires_modalidade: bool
    sync_type: str = "incremental"


# ALL 12 PNCP ENDPOINTS - Clean definitions without duplicates
ENDPOINT_CONFIG: Dict[str, EndpointConfig] = {
    # Phase 1: Core Publication Endpoints
    "contratacoes_publicacao": EndpointConfig(
        path="/v1/contratacoes/publicacao",
        required_params=["dataInicial", "dataFinal", "codigoModalidadeContratacao", "pagina"],
        optional_params=["codigoModoDisputa", "uf", "codigoMunicipioIbge", "cnpj", "codigoUnidadeAdministrativa", "idUsuario", "tamanhoPagina"],
        page_size_limits=PageSizeLimits(min=10, max=50),
        default_page_size=50,
        priority=1,
        requires_modalidade=True,
    ),
    "contratos": EndpointConfig(
        path="/v1/contratos",
        required_params=["dataInicial", "dataFinal", "pagina"],
        optional_params=["cnpjOrgao", "codigoUnidadeAdministrativa", "usuarioId", "tamanhoPagina"],
        page_size_limits=PageSizeLimits(min=10, max=500),
        default_page_size=500,
        priority=2,
        requires_modalidade=False,
    ),
    "atas": EndpointConfig(
        path="/v1/atas",
        required_params=["dataInicial", "dataFinal", "pagina"],
        optional_params=["idUsuario", "cnpj", "codigoUnidadeAdministrativa", "tamanhoPagina"],
        page_size_limits=PageSizeLimits(min=10, max=500),
        default_page_size=500,
        priority=3,
        requires_modalidade=False,
    ),
    
    # Phase 2: Update/Sync Endpoints
    "contratacoes_atualizacao": EndpointConfig(
        path="/v1/contratacoes/atualizacao",
        required_params=["dataInicial", "dataFinal", "codigoModalidadeContratacao", "pagina"],
        optional_params=["codigoModoDisputa", "uf", "codigoMunicipioIbge", "cnpj", "codigoUnidadeAdministrativa", "idUsuario", "tamanhoPagina"],
        page_size_limits=PageSizeLimits(min=10, max=50),
        default_page_size=50,
        priority=4,
        requires_modalidade=True,
        sync_type="incremental",
    ),
    "contratos_atualizacao": EndpointConfig(
        path="/v1/contratos/atualizacao",
        required_params=["dataInicial", "dataFinal", "pagina"],
        optional_params=["cnpjOrgao", "codigoUnidadeAdministrativa", "usuarioId", "tamanhoPagina"],
        page_size_limits=PageSizeLimits(min=10, max=500),
        default_page_size=500,
        priority=5,
        requires_modalidade=False,
        sync_type="incremental",
    ),
    "atas_atualizacao": EndpointConfig(
        path="/v1/atas/atualizacao",
        required_params=["dataInicial", "dataFinal", "pagina"],
        optional_params=["idUsuario", "cnpj", "codigoUnidadeAdministrativa", "tamanhoPagina"],
        page_size_limits=PageSizeLimits(min=10, max=500),
        default_page_size=500,
        priority=6,
        requires_modalidade=False,
        sync_type="incremental",
    ),
    
    # Phase 3: Specialized Endpoints
    "contratacoes_proposta": EndpointConfig(
        path="/v1/contratacoes/proposta",
        required_params=["dataFinal", "pagina"],
        optional_params=["codigoModalidadeContratacao", "uf", "codigoMunicipioIbge", "cnpj", "codigoUnidadeAdministrativa", "idUsuario", "tamanhoPagina"],
        page_size_limits=PageSizeLimits(min=10, max=500),
        default_page_size=500,
        priority=7,
        requires_modalidade=False,
        sync_type="snapshot",
    ),
    "instrumentoscobranca_inclusao": EndpointConfig(
        path="/v1/instrumentoscobranca/inclusao",
        required_params=["dataInicial", "dataFinal", "pagina"],
        optional_params=["tipoInstrumentoCobranca", "cnpjOrgao", "tamanhoPagina"],
        page_size_limits=PageSizeLimits(min=10, max=100),
        default_page_size=100,
        priority=8,
        requires_modalidade=False,
        sync_type="incremental",
    ),
    
    # Phase 4: PCA (Plano de Contratação Anual) Endpoints
    "pca": EndpointConfig(
        path="/v1/pca/",
        required_params=["anoPca", "codigoClassificacaoSuperior", "pagina"],
        optional_params=["tamanhoPagina"],
        page_size_limits=PageSizeLimits(min=10, max=500),
        default_page_size=500,
        priority=9,
        requires_modalidade=False,
        sync_type="annual",
    ),
    "pca_usuario": EndpointConfig(
        path="/v1/pca/usuario",
        required_params=["anoPca", "idUsuario", "pagina"],
        optional_params=["codigoClassificacaoSuperior", "cnpj", "tamanhoPagina"],
        page_size_limits=PageSizeLimits(min=10, max=500),
        default_page_size=500,
        priority=10,
        requires_modalidade=False,
        sync_type="annual",
    ),
    "pca_atualizacao": EndpointConfig(
        path="/v1/pca/atualizacao",
        required_params=["dataInicio", "dataFim", "pagina"],
        optional_params=["cnpj", "codigoUnidade", "tamanhoPagina"],
        page_size_limits=PageSizeLimits(min=10, max=500),
        default_page_size=500,
        priority=11,
        requires_modalidade=False,
        sync_type="incremental",
    ),
    
    # Phase 5: Detail/Drill-down Endpoints  
    # TODO: Verify if the dlt `rest_api_source` can handle path parameters
    # out-of-the-box with this configuration. Endpoints with dynamic path segments
    # like `/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}` often require a custom
    # dlt resource class that can format the URL dynamically for each item.
    # If the current config doesn't work, this will need to be implemented as a
    # separate, specialized resource.
    "contratacao_especifica": EndpointConfig(
        path="/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}",
        required_params=["cnpj", "ano", "sequencial"],
        optional_params=[],
        page_size_limits=PageSizeLimits(min=1, max=1),  # Single record endpoint
        default_page_size=1,
        priority=12,
        requires_modalidade=False,
        sync_type="on_demand",
    ),
}

settings = Settings()