"""Configuration management for the baliza package."""

import yaml
from pathlib import Path
from typing import Any, Dict
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, UUID4

from baliza.enums.contratacao import ModalidadeContratacao


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
    concurrency: int = 8
    rate_limit_delay: float = 0.0
    page_size: int = 500
    request_timeout: int = 30
    user_agent: str = "BALIZA/3.0 (Backup Aberto de Licitacoes)"
    internet_archive_identifier: str = "baliza-pncp"
    ia_access_key: str = ""
    ia_secret_key: str = ""
    baliza_namespace: UUID4 = Field(default="169643d2-3103-492e-9189-53644ab43f4b")

settings = Settings()

def load_config(config_path: Path = None) -> Dict[str, Any]:
    """Load and parse endpoints configuration from YAML."""
    if config_path is None:
        config_path = Path(__file__).parent.parent.parent / "config" / "endpoints.yaml"

    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def get_endpoint_config(endpoint_name: str, config_path: Path = None) -> Dict[str, Any]:
    """Get configuration for a specific endpoint."""
    config = load_config(config_path)

    endpoints = config.get('endpoints', {})
    if endpoint_name not in endpoints:
        raise KeyError(f"Endpoint '{endpoint_name}' not found in configuration")

    endpoint_config = endpoints[endpoint_name].copy()
    defaults = config.get('default_settings', {})

    for key, default_value in defaults.items():
        if key not in endpoint_config:
            endpoint_config[key] = default_value

    return endpoint_config

def get_all_active_endpoints(config_path: Path = None) -> Dict[str, Dict[str, Any]]:
    """Get all active endpoint configurations."""
    config = load_config(config_path)

    active_endpoints = {}
    defaults = config.get('default_settings', {})

    for name, endpoint_config in config.get('endpoints', {}).items():
        if endpoint_config.get('active', True):
            merged_config = defaults.copy()
            merged_config.update(endpoint_config)
            active_endpoints[name] = merged_config

    return active_endpoints
