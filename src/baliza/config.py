from typing import Dict, List

from pydantic import BaseModel
from pydantic_settings import BaseSettings

from .enums import ModalidadeContratacao


class PNCPAPISettings(BaseSettings):
    """API settings for PNCP."""

    base_url: str = "https://pncp.gov.br/api/consulta"
    supported_api_version: str = "1.0"
    timeout_seconds: int = 30

    class Config:
        env_file = ".env"
        env_prefix = "PNCP_"


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

    # Endpoints
    phase_2a_endpoints: List[str] = ["contratacoes_publicacao", "contratos", "atas"]
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
    ]

    # High Priority Modalidades
    high_priority_modalidades: List[ModalidadeContratacao] = [
        ModalidadeContratacao.PREGAO_ELETRONICO,
        ModalidadeContratacao.PREGAO_PRESENCIAL,
        ModalidadeContratacao.DISPENSA_DE_LICITACAO,
        ModalidadeContratacao.CONCORRENCIA_ELETRONICA,
        ModalidadeContratacao.CONCORRENCIA_PRESENCIAL,
    ]

    # Pagination
    default_page_size: int = 500
    MAX_PAGE_SIZE: int = 500

    # Schema Validation
    VALIDATE_SCHEMA: bool = True
    SCHEMA_FINGERPRINT_CHECK: bool = True

    # Data Retention
    retention_days_raw: int = 365
    retention_days_staging: int = 180
    retention_days_logs: int = 90

    # Security
    secret_key: str = "your-secret-key"
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


ENDPOINT_CONFIG: Dict[str, EndpointConfig] = {
    "contratacoes_publicacao": EndpointConfig(
        path="/v1/contratacoes/publicacao",
        required_params=[
            "dataInicial",
            "dataFinal",
            "codigoModalidadeContratacao",
            "pagina",
        ],
        optional_params=[
            "codigoModoDisputa",
            "uf",
            "codigoMunicipioIbge",
            "cnpj",
            "codigoUnidadeAdministrativa",
            "idUsuario",
            "tamanhoPagina",
        ],
        page_size_limits=PageSizeLimits(min=10, max=50),
        default_page_size=50,
        priority=1,
        requires_modalidade=True,
    ),
    "contratos": EndpointConfig(
        path="/v1/contratos",
        required_params=["dataInicial", "dataFinal", "pagina"],
        optional_params=[
            "cnpjOrgao",
            "codigoUnidadeAdministrativa",
            "usuarioId",
            "tamanhoPagina",
        ],
        page_size_limits=PageSizeLimits(min=10, max=500),
        default_page_size=500,
        priority=2,
        requires_modalidade=False,
    ),
    "atas": EndpointConfig(
        path="/v1/atas",
        required_params=["dataInicial", "dataFinal", "pagina"],
        optional_params=[
            "idUsuario",
            "cnpj",
            "codigoUnidadeAdministrativa",
            "tamanhoPagina",
        ],
        page_size_limits=PageSizeLimits(min=10, max=500),
        default_page_size=500,
        priority=3,
        requires_modalidade=False,
    ),
    "contratacoes_atualizacao": EndpointConfig(
        path="/v1/contratacoes/atualizacao",
        required_params=[
            "dataInicial",
            "dataFinal",
            "codigoModalidadeContratacao",
            "pagina",
        ],
        optional_params=[
            "codigoModoDisputa",
            "uf",
            "codigoMunicipioIbge",
            "cnpj",
            "codigoUnidadeAdministrativa",
            "idUsuario",
            "tamanhoPagina",
        ],
        page_size_limits=PageSizeLimits(min=10, max=50),
        default_page_size=50,
        priority=4,
        requires_modalidade=True,
        sync_type="incremental",
    ),
    "contratos_atualizacao": EndpointConfig(
        path="/v1/contratos/atualizacao",
        required_params=["dataInicial", "dataFinal", "pagina"],
        optional_params=[
            "cnpjOrgao",
            "codigoUnidadeAdministrativa",
            "usuarioId",
            "tamanhoPagina",
        ],
        page_size_limits=PageSizeLimits(min=10, max=500),
        default_page_size=500,
        priority=5,
        requires_modalidade=False,
        sync_type="incremental",
    ),
    "atas_atualizacao": EndpointConfig(
        path="/v1/atas/atualizacao",
        required_params=["dataInicial", "dataFinal", "pagina"],
        optional_params=[
            "idUsuario",
            "cnpj",
            "codigoUnidadeAdministrativa",
            "tamanhoPagina",
        ],
        page_size_limits=PageSizeLimits(min=10, max=500),
        default_page_size=500,
        priority=6,
        requires_modalidade=False,
        sync_type="incremental",
    ),
    "contratacoes_proposta": EndpointConfig(
        path="/v1/contratacoes/proposta",
        required_params=["dataFinal", "pagina"],
        optional_params=[
            "codigoModalidadeContratacao",
            "uf",
            "codigoMunicipioIbge",
            "cnpj",
            "codigoUnidadeAdministrativa",
            "idUsuario",
            "tamanhoPagina",
        ],
        page_size_limits=PageSizeLimits(min=10, max=50),
        default_page_size=50,
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
        optional_params=[
            "codigoClassificacaoSuperior",
            "cnpj",
            "tamanhoPagina",
        ],
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
}

settings = Settings()
