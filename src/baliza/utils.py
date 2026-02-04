"""Validation utilities."""

import ipaddress
import os
import re
import socket
from urllib.parse import urlparse


def validate_url(url: str) -> str:
    """Validate that a URL points to a public IP address (SSRF protection).

    Args:
        url: The URL to validate.

    Returns:
        The validated URL.

    Raises:
        ValueError: If the URL is invalid or points to a private network.
    """
    if not url:
        raise ValueError("URL cannot be empty.")

    try:
        parsed = urlparse(url)
    except Exception as e:
        raise ValueError(f"Invalid URL format: {e}") from e

    if parsed.scheme not in ("http", "https"):
        raise ValueError("URL must use http or https scheme.")

    if not parsed.hostname:
        raise ValueError("URL must have a hostname.")

    # Allow bypass via env var
    if os.getenv("BALIZA_ALLOW_PRIVATE_NETWORKS") == "1":
        return url

    hostname = parsed.hostname

    # Handle IPv6 literals in hostname (remove brackets)
    if hostname.startswith("[") and hostname.endswith("]"):
        hostname = hostname[1:-1]

    try:
        # Resolve hostname to IP addresses
        # We use getaddrinfo to handle both IPv4 and IPv6
        addr_info = socket.getaddrinfo(hostname, None)
    except socket.gaierror as e:
        raise ValueError(f"Could not resolve hostname '{hostname}': {e}") from e

    for _, _, _, _, sockaddr in addr_info:
        ip_str = sockaddr[0]
        try:
            ip_obj = ipaddress.ip_address(ip_str)
        except ValueError:
            # Should not happen with valid getaddrinfo result
            continue

        if ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_link_local:
            raise ValueError(f"URL resolves to private/local IP: {ip_str}")

    return url


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


def escape_sql_literal(value: str) -> str:
    """Escape a string for use in a SQL literal (doubling single quotes).

    Args:
        value: The string to escape.

    Returns:
        The escaped string.
    """
    return value.replace("'", "''")
