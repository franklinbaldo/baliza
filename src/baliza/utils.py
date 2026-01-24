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


def validate_resource_path(path: str) -> str:
    """Validate that a resource path is safe (no traversal).

    Args:
        path: The resource path to validate.

    Returns:
        The validated path.

    Raises:
        ValueError: If the path contains traversal attempts or invalid characters.
    """
    if ".." in path:
        raise ValueError("Invalid resource path: must not contain '..'")

    # Allow alphanumeric, underscore, hyphen, and forward slash.
    if not re.match(r"^[a-zA-Z0-9_/-]+$", path):
        raise ValueError(
            f"Invalid resource path: '{path}'. Must be alphanumeric with underscores, hyphens, and slashes only."
        )

    return path
