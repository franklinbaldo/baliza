from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    # API Configuration
    PNCP_API_BASE_URL: str = "https://pncp.gov.br/api/consulta"
    SUPPORTED_API_VERSION: str = "1.0"
    API_TIMEOUT_SECONDS: int = 30

    # Database Configuration
    DATABASE_PATH: str = "data/baliza.duckdb"

    # Rate Limiting (PNCP has no explicit rate limits - using conservative approach)
    REQUESTS_PER_MINUTE: int = 120  # Increased - no explicit limits found
    REQUESTS_PER_HOUR: int = 7200  # Increased - 120 * 60
    CONCURRENT_ENDPOINTS: int = 12  # All endpoints can run simultaneously

    # Retry Configuration
    MAX_RETRY_ATTEMPTS: int = 3
    RETRY_BACKOFF_FACTOR: float = 2.0
    RETRY_BACKOFF_MAX: int = 300

    # Circuit Breaker
    CIRCUIT_BREAKER_FAILURE_THRESHOLD: int = 5
    CIRCUIT_BREAKER_RECOVERY_TIMEOUT: int = 300

    # Default Date Ranges
    DEFAULT_DATE_RANGE_DAYS: int = 7
    MAX_DATE_RANGE_DAYS: int = 30

    # Phase 2A Priority Endpoints
    PHASE_2A_ENDPOINTS: List[str] = ["contratacoes_publicacao", "contratos", "atas"]

    # High Priority Modalidades (Pregão, Dispensa, Concorrência)
    HIGH_PRIORITY_MODALIDADES: List[int] = [6, 7, 8, 4, 5]

    # Pagination
    DEFAULT_PAGE_SIZE: int = 500
    MAX_PAGE_SIZE: int = 500

    # Schema Validation
    VALIDATE_SCHEMA: bool = True
    SCHEMA_FINGERPRINT_CHECK: bool = True
    
    # TODO: Add configuration for data retention policies
    # TODO: Add configuration for security settings (encryption, auth)
    # TODO: Add configuration for monitoring and alerting thresholds

    class Config:
        env_file = ".env"


# Endpoint Configuration Mapping
ENDPOINT_CONFIG = {
    "contratacoes_publicacao": {
        "path": "/v1/contratacoes/publicacao",
        "required_params": [
            "dataInicial",
            "dataFinal",
            "codigoModalidadeContratacao",
            "pagina",
        ],
        "optional_params": [
            "codigoModoDisputa",
            "uf",
            "codigoMunicipioIbge",
            "cnpj",
            "codigoUnidadeAdministrativa",
            "idUsuario",
            "tamanhoPagina",
        ],
        "page_size_limits": {"min": 10, "max": 50},
        "default_page_size": 50,
        "priority": 1,
        "requires_modalidade": True,
    },
    "contratos": {
        "path": "/v1/contratos",
        "required_params": ["dataInicial", "dataFinal", "pagina"],
        "optional_params": [
            "cnpjOrgao",
            "codigoUnidadeAdministrativa",
            "usuarioId",
            "tamanhoPagina",
        ],
        "page_size_limits": {"min": 10, "max": 500},
        "default_page_size": 500,
        "priority": 2,
        "requires_modalidade": False,
    },
    "atas": {
        "path": "/v1/atas",
        "required_params": ["dataInicial", "dataFinal", "pagina"],
        "optional_params": [
            "idUsuario",
            "cnpj",
            "codigoUnidadeAdministrativa",
            "tamanhoPagina",
        ],
        "page_size_limits": {"min": 10, "max": 500},
        "default_page_size": 500,
        "priority": 3,
        "requires_modalidade": False,
    },
    "contratacoes_atualizacao": {
        "path": "/v1/contratacoes/atualizacao",
        "required_params": [
            "dataInicial",
            "dataFinal",
            "codigoModalidadeContratacao",
            "pagina",
        ],
        "optional_params": [
            "codigoModoDisputa",
            "uf",
            "codigoMunicipioIbge",
            "cnpj",
            "codigoUnidadeAdministrativa",
            "idUsuario",
            "tamanhoPagina",
        ],
        "page_size_limits": {"min": 10, "max": 50},
        "default_page_size": 50,
        "priority": 4,
        "requires_modalidade": True,
        "sync_type": "incremental",
    },
    "contratos_atualizacao": {
        "path": "/v1/contratos/atualizacao",
        "required_params": ["dataInicial", "dataFinal", "pagina"],
        "optional_params": [
            "cnpjOrgao",
            "codigoUnidadeAdministrativa",
            "usuarioId",
            "tamanhoPagina",
        ],
        "page_size_limits": {"min": 10, "max": 500},
        "default_page_size": 500,
        "priority": 5,
        "requires_modalidade": False,
        "sync_type": "incremental",
    },
    "atas_atualizacao": {
        "path": "/v1/atas/atualizacao",
        "required_params": ["dataInicial", "dataFinal", "pagina"],
        "optional_params": [
            "idUsuario",
            "cnpj",
            "codigoUnidadeAdministrativa",
            "tamanhoPagina",
        ],
        "page_size_limits": {"min": 10, "max": 500},
        "default_page_size": 500,
        "priority": 6,
        "requires_modalidade": False,
        "sync_type": "incremental",
    },
}

# Domain Tables
MODALIDADE_CONTRATACAO = {
    1: "Leilão - Eletrônico",
    2: "Diálogo Competitivo",
    3: "Concurso",
    4: "Concorrência - Eletrônica",
    5: "Concorrência - Presencial",
    6: "Pregão - Eletrônico",
    7: "Pregão - Presencial",
    8: "Dispensa de Licitação",
    9: "Inexigibilidade",
    10: "Manifestação de Interesse",
    11: "Pré-qualificação",
    12: "Credenciamento",
    13: "Leilão - Presencial",
}

SITUACAO_CONTRATACAO = {
    1: "Divulgada no PNCP",
    2: "Revogada",
    3: "Anulada",
    4: "Suspensa",
}

settings = Settings()
