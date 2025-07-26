"""
Utility functions for BALIZA.
Centralized collection of general-purpose helper functions.
"""

import hashlib
import json
from typing import Any


def hash_sha256(data: Any) -> str:
    """
    Create SHA256 hash of the given data for deduplication.
    
    SHA256 is used to ensure minimal collision risk for large PNCP datasets
    while maintaining data integrity across the pipeline.
    
    Args:
        data: Any JSON-serializable data
        
    Returns:
        Hexadecimal hash string
    """
    json_str = json.dumps(data, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(json_str.encode('utf-8')).hexdigest()