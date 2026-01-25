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
    """Validate that a string is a safe URL resource path.

    Allowed characters: alphanumeric, underscore, hyphen, slash.
    Explicitly denies traversal (..) or start with slash.

    Args:
        path: The resource path to validate.

    Returns:
        The validated path.

    Raises:
        ValueError: If the path is invalid.
    """
    if ".." in path:
        raise ValueError("Path traversal detected: '..'")

    if path.startswith("/"):
        raise ValueError("Resource path must be relative (cannot start with '/')")

    if not re.match(r"^[a-zA-Z0-9_\-/]+$", path):
        raise ValueError(
            f"Invalid resource path: '{path}'. "
            "Must be alphanumeric with underscores, hyphens, and slashes."
        )
    return path
