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


from .config import get_endpoint_config, get_all_active_endpoints, load_config

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
        self.config_path = Path(config_path) if config_path else None
        
    def load_config(self) -> Dict[str, Any]:
        """Load and parse endpoints configuration."""
        return load_config(self.config_path)
    
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

