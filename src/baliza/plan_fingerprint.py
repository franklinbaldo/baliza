"""Plan fingerprinting for dbt-driven task planning.

Implements SHA256 fingerprinting to detect configuration drift between
dbt planning models and Python execution environment.

ADR-009: dbt-Driven Task Planning Architecture
"""

import hashlib
import json
from pathlib import Path
from typing import Any, Dict

import yaml


class PlanFingerprint:
    """Manages plan fingerprinting for configuration drift detection.
    
    Creates deterministic SHA256 hashes of configuration that can be compared
    between dbt macro execution and Python runtime to ensure consistency.
    """
    
    def __init__(self, config_path: str = None):
        """Initialize fingerprint generator.
        
        Args:
            config_path: Path to unified endpoints configuration file
        """
        if config_path is None:
            config_path = str(Path(__file__).parent.parent.parent / "config" / "endpoints.yaml")
        self.config_path = Path(config_path)
        
    def load_config(self) -> Dict[str, Any]:
        """Load and parse endpoints configuration."""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
            
        with open(self.config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def _filter_for_fingerprint(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Remove environment-specific and secret values from config.
        
        This ensures fingerprints remain stable across environments while
        detecting actual configuration changes.
        """
        filtered = config.copy()
        
        # Remove excluded sections per fingerprint_excludes
        excludes = config.get('fingerprint_excludes', [])
        for exclude_path in excludes:
            keys = exclude_path.split('.')
            current = filtered
            for key in keys[:-1]:
                if key in current:
                    current = current[key]
                else:
                    break
            else:
                # All intermediate keys exist, remove the final key
                if keys[-1] in current:
                    del current[keys[-1]]
        
        # Sort endpoints by name for deterministic fingerprinting
        if 'endpoints' in filtered:
            filtered['endpoints'] = dict(sorted(filtered['endpoints'].items()))
            
            # Sort modalidades arrays for consistency
            for endpoint_name, endpoint_config in filtered['endpoints'].items():
                if 'modalidades' in endpoint_config and isinstance(endpoint_config['modalidades'], list):
                    endpoint_config['modalidades'] = sorted(endpoint_config['modalidades'])
        
        return filtered
    
    def generate_fingerprint(self, date_range_start: str = None, date_range_end: str = None, environment: str = "prod") -> str:
        """Generate SHA256 fingerprint for current configuration.
        
        Args:
            date_range_start: Start date for extraction plan (YYYY-MM-DD)
            date_range_end: End date for extraction plan (YYYY-MM-DD)
            environment: Environment name for fingerprinting
            
        Returns:
            SHA256 hash as hex string
        """
        config = self.load_config()
        filtered_config = self._filter_for_fingerprint(config)
        
        # Include environment and date range in fingerprint
        fingerprint_data = {
            'config': filtered_config,
            'date_range_start': date_range_start,
            'date_range_end': date_range_end,
            'environment': environment,
            'version': config.get('version', '1.0')
        }
        
        # Create deterministic JSON representation
        json_str = json.dumps(fingerprint_data, sort_keys=True, separators=(',', ':'))
        
        # Generate SHA256 hash
        return hashlib.sha256(json_str.encode('utf-8')).hexdigest()
    
    def validate_fingerprint(self, expected_fingerprint: str, **kwargs) -> bool:
        """Validate that current config matches expected fingerprint.
        
        Args:
            expected_fingerprint: SHA256 hash from dbt plan metadata
            **kwargs: Arguments passed to generate_fingerprint()
            
        Returns:
            True if fingerprints match, False otherwise
        """
        current_fingerprint = self.generate_fingerprint(**kwargs)
        return current_fingerprint == expected_fingerprint
    
    def get_plan_metadata(self, date_range_start: str, date_range_end: str, environment: str = "prod") -> Dict[str, Any]:
        """Generate complete plan metadata for dbt models.
        
        Returns:
            Dictionary with fingerprint and metadata for task_plan_meta table
        """
        config = self.load_config()
        fingerprint = self.generate_fingerprint(date_range_start, date_range_end, environment)
        
        # Count active endpoints
        active_endpoints = [
            name for name, endpoint in config.get('endpoints', {}).items()
            if endpoint.get('active', True)
        ]
        
        return {
            'plan_fingerprint': fingerprint,
            'config_version': config.get('version', '1.0'),
            'environment': environment,
            'date_range_start': date_range_start,
            'date_range_end': date_range_end,
            'active_endpoints': active_endpoints,
            'endpoint_count': len(active_endpoints)
        }


def get_endpoint_config(endpoint_name: str, config_path: str = None) -> Dict[str, Any]:
    """Get configuration for a specific endpoint.
    
    Helper function for Python extraction code to access unified config.
    
    Args:
        endpoint_name: Name of the endpoint to retrieve
        config_path: Path to configuration file
        
    Returns:
        Endpoint configuration dictionary
        
    Raises:
        KeyError: If endpoint not found in configuration
    """
    fingerprint = PlanFingerprint(config_path)
    config = fingerprint.load_config()
    
    endpoints = config.get('endpoints', {})
    if endpoint_name not in endpoints:
        raise KeyError(f"Endpoint '{endpoint_name}' not found in configuration")
    
    # Merge with default settings
    endpoint_config = endpoints[endpoint_name].copy()
    defaults = config.get('default_settings', {})
    
    # Apply defaults for missing values
    for key, default_value in defaults.items():
        if key not in endpoint_config:
            endpoint_config[key] = default_value
    
    return endpoint_config


def get_all_active_endpoints(config_path: str = None) -> Dict[str, Dict[str, Any]]:
    """Get all active endpoint configurations.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Dictionary mapping endpoint names to their configurations
    """
    fingerprint = PlanFingerprint(config_path)
    config = fingerprint.load_config()
    
    active_endpoints = {}
    defaults = config.get('default_settings', {})
    
    for name, endpoint_config in config.get('endpoints', {}).items():
        if endpoint_config.get('active', True):
            # Merge with defaults
            merged_config = defaults.copy()
            merged_config.update(endpoint_config)
            active_endpoints[name] = merged_config
    
    return active_endpoints