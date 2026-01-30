"""Validation utilities."""

import ipaddress
import os
import re
from urllib.parse import urlparse


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
    """Validate that a string is a safe resource path (alphanumeric, -, _, /).

    Args:
        path: The resource path to validate.

    Returns:
        The validated path.

    Raises:
        ValueError: If the path contains invalid characters or traversal attempts.
    """
    if not re.match(r"^[a-zA-Z0-9_\-/]+$", path):
        raise ValueError(f"Invalid resource path: '{path}'. Must be alphanumeric with '-', '_', or '/'.")

    if ".." in path:
        raise ValueError(f"Invalid resource path: '{path}'. Path traversal ('..') is not allowed.")

    return path


def validate_url(url: str) -> str:
    """Validate that a URL is safe (HTTP/HTTPS) and not targeting private networks (SSRF protection).

    Args:
        url: The URL to validate.

    Returns:
        The validated URL.

    Raises:
        ValueError: If the URL is invalid or points to a private network.
    """
    parsed = urlparse(url)

    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"Invalid URL scheme: '{parsed.scheme}'. Must be 'http' or 'https'.")

    if not parsed.hostname:
        raise ValueError(f"Invalid URL: '{url}'. Must have a hostname.")

    # Allow bypassing private network check via environment variable
    if os.environ.get("BALIZA_ALLOW_PRIVATE_NETWORKS") == "1":
        return url

    hostname = parsed.hostname.lower()

    if hostname == "localhost":
        raise ValueError(f"Invalid URL: '{url}'. Access to localhost is restricted.")

    try:
        ip = ipaddress.ip_address(hostname)
    except ValueError:
        # Not an IP address, so it's a domain name.
        return url

    if ip.is_private or ip.is_loopback or ip.is_link_local:
        raise ValueError(
            f"Invalid URL: '{url}'. Access to private network '{hostname}' is restricted."
        )

    return url
