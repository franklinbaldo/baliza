"""Configuration constants for the baliza package."""

from pydantic_settings import BaseSettings, SettingsConfigDict

from baliza.enums import ModalidadeContratacao


class Settings(BaseSettings):
    """Manages application configuration using Pydantic BaseSettings.
    Allows for environment-aware configuration management, essential for
    flexible deployment and testing scenarios (e.g., staging vs. production).
    Attributes:
        pncp_base_url (str): The base URL for the PNCP API.
        concurrency (int): The number of concurrent requests to make to the PNCP API.
        request_timeout (int): The timeout in seconds for HTTP requests.
        user_agent (str): The User-Agent header to use for HTTP requests.
        pncp_endpoints (list): A list of dictionaries defining the PNCP API endpoints to query.
    """

    model_config = SettingsConfigDict(
        env_prefix="BALIZA_", env_file=".env", extra="ignore"
    )

    pncp_base_url: str = "https://pncp.gov.br/api/consulta"
    concurrency: int = 8  # Optimal based on stress testing - PNCP handles this well
    rate_limit_delay: float = (
        0.0  # No delay needed - PNCP API is more tolerant than expected
    )
    page_size: int = 500
    request_timeout: int = 30
    user_agent: str = "BALIZA/3.0 (Backup Aberto de Licitacoes)"
    internet_archive_identifier: str = "baliza-pncp"
    ia_access_key: str = ""
    ia_secret_key: str = ""
    # Working endpoints (only the reliable ones) - OpenAPI compliant
    pncp_endpoints: list = [
        {
            "name": "contratos_publicacao",
            "path": "/v1/contratos",
            "description": "Contratos por Data de Publicação",
            "date_params": ["dataInicial", "dataFinal"],
            "max_days": 365,  # API limit, but we use monthly chunks
            "supports_date_range": True,
            "page_size": 500,  # OpenAPI spec: max 500 for this endpoint
        },
        {
            "name": "contratos_atualizacao",
            "path": "/v1/contratos/atualizacao",
            "description": "Contratos por Data de Atualização Global",
            "date_params": ["dataInicial", "dataFinal"],
            "max_days": 365,  # API limit, but we use monthly chunks
            "supports_date_range": True,
            "page_size": 500,  # OpenAPI spec: max 500 for this endpoint
        },
        {
            "name": "atas_periodo",
            "path": "/v1/atas",
            "description": "Atas de Registro de Preço por Período de Vigência",
            "date_params": ["dataInicial", "dataFinal"],
            "max_days": 365,  # API limit, but we use monthly chunks
            "supports_date_range": True,
            "page_size": 500,  # OpenAPI spec: max 500 for this endpoint
        },
        {
            "name": "atas_atualizacao",
            "path": "/v1/atas/atualizacao",
            "description": "Atas por Data de Atualização Global",
            "date_params": ["dataInicial", "dataFinal"],
            "max_days": 365,  # API limit, but we use monthly chunks
            "supports_date_range": True,
            "page_size": 500,  # OpenAPI spec: max 500 for this endpoint
        },
        {
            "name": "contratacoes_publicacao",
            "path": "/v1/contratacoes/publicacao",
            "description": "Contratações por Data de Publicação",
            "date_params": ["dataInicial", "dataFinal"],
            "max_days": 365,
            "supports_date_range": True,
            "iterate_modalidades": [m.value for m in ModalidadeContratacao],
            "page_size": 50,  # OpenAPI spec: max 50 for contratacoes endpoints
        },
        {
            "name": "contratacoes_atualizacao",
            "path": "/v1/contratacoes/atualizacao",
            "description": "Contratações por Data de Atualização Global",
            "date_params": ["dataInicial", "dataFinal"],
            "max_days": 365,
            "supports_date_range": True,
            "iterate_modalidades": [m.value for m in ModalidadeContratacao],
            "page_size": 50,  # OpenAPI spec: max 50 for contratacoes endpoints
        },
        {
            "name": "pca_atualizacao",
            "path": "/v1/pca/atualizacao",
            "description": "PCA por Data de Atualização Global",
            "date_params": [
                "dataInicio",
                "dataFim",
            ],  # PCA uses different parameter names
            "max_days": 365,
            "supports_date_range": True,
            "page_size": 500,  # OpenAPI spec: max 500 for this endpoint
        },
        {
            "name": "instrumentoscobranca_inclusao",
            "path": "/v1/instrumentoscobranca/inclusao",  # Correct path from OpenAPI spec
            "description": "Instrumentos de Cobrança por Data de Inclusão",
            "date_params": ["dataInicial", "dataFinal"],  # Uses date range
            "max_days": 365,
            "supports_date_range": True,  # Date range endpoint
            "page_size": 100,  # OpenAPI spec: max 100, min 10 for this endpoint
            "min_page_size": 10,  # Minimum page size required
        },
        {
            "name": "contratacoes_proposta",
            "path": "/v1/contratacoes/proposta",
            "description": "Contratações com Recebimento de Propostas Aberto",
            "date_params": ["dataFinal"],
            "max_days": 365,
            "supports_date_range": False,
            "requires_single_date": True,  # This endpoint doesn't use date chunking
            "requires_future_date": True,  # This endpoint needs current/future dates
            "future_days_offset": 1825,  # Use 5 years in the future to capture most active contracts
            # No iterate_modalidades - captures more data without it
            "page_size": 50,  # OpenAPI spec: max 50 for contratacoes endpoints
        },
        # Note: PCA usuario endpoint requires anoPca and idUsuario parameters
        # This is commented out as it requires specific user/org data to be useful
        # {
        #     "name": "pca_usuario",
        #     "path": "/v1/pca/usuario",
        #     "description": "PCA por Usuário e Ano",
        #     "date_params": [],  # Uses anoPca instead of date ranges
        #     "max_days": 0,
        #     "supports_date_range": False,
        #     "requires_specific_params": True,  # Requires anoPca, idUsuario
        #     "extra_params": {"anoPca": 2024, "idUsuario": "example"},
        #     "page_size": 500,
        # },
    ]


settings = Settings()
