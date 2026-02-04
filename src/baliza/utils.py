"""Validation utilities."""

import re

def validate_identifier(name: str) -> str:
    """Validate that a string is a safe SQL identifier (alphanumeric + underscore).

    Args:
        name: The identifier to validate.

    Returns:
        The validated identifier.

    Raises:
        ValueError: If the identifier is invalid.
    """
    if not re.match(r"^[a-zA-Z0-9_]+$", name):
        raise ValueError(f"Invalid identifier: '{name}'. Must be alphanumeric with underscores only.")
    return name
