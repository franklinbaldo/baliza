"""Validation utilities."""

import ipaddress
import os
import re
import socket
from urllib.parse import urlparse


def validate_url(url: str) -> str:
    """Validate that a URL points to a global, unicast IP address (SSRF protection).

    Args:
        url: The URL to validate.

    Returns:
        The validated URL.

    Raises:
        ValueError: If the URL is invalid or points to a private/non-global network.
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

        # Ensure the IP is globally reachable and not multicast
        if not ip_obj.is_global or ip_obj.is_multicast:
            raise ValueError(f"URL resolves to non-global or multicast IP: {ip_str}")

    return url


def validate_identifier(name: str, max_length: int = 64) -> str:
    """Validate that a string is a safe SQL identifier (alphanumeric + underscore).

    Args:
        name: The identifier to validate.
        max_length: Maximum allowed length (default: 64).

    Returns:
        The validated identifier.

    Raises:
        ValueError: If the identifier is invalid or too long.
    """
    if len(name) > max_length:
        raise ValueError(f"Invalid identifier: '{name}'. Length {len(name)} exceeds limit of {max_length}.")

    if not re.match(r"^[a-zA-Z0-9_]+$", name):
        raise ValueError(f"Invalid identifier: '{name}'. Must be alphanumeric with underscores only.")
    return name


def validate_resource_path(path: str, max_length: int = 255) -> str:
    """Validate that a string is a safe resource path (alphanumeric, -, _, /).

    Args:
        path: The resource path to validate.
        max_length: Maximum allowed length (default: 255).

    Returns:
        The validated path.

    Raises:
        ValueError: If the path contains invalid characters, traversal attempts, or is too long.
    """
    if len(path) > max_length:
        raise ValueError(f"Invalid resource path: '{path}'. Length {len(path)} exceeds limit of {max_length}.")

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


def scrub_url_params(text: str) -> str:
    """Mask credentials and query parameters in URLs found in text to prevent logging secrets.

    Args:
        text: The text containing URLs to scrub.

    Returns:
        The text with credentials and query parameters replaced by '***'.
    """
    # 1. Scrub credentials in authority: scheme://user:pass@host
    # ([a-zA-Z][a-zA-Z0-9+.-]*://) : Group 1 - Scheme + ://
    # ([^/@\s'\"]+)                : Group 2 - User:pass (no slash, at, space, quotes)
    # (@)                          : Group 3 - @ separator
    text = re.sub(
        r"([a-zA-Z][a-zA-Z0-9+.-]*://)([^/@\s'\"]+)(@)", r"\1***\3", text
    )

    # 2. Scrub query parameters: scheme://path?query
    # ([a-zA-Z][a-zA-Z0-9+.-]*://[^\s'\"?]+) : Group 1 - Scheme + path (no space, quote, ?)
    # (\?[^\s'\"]*)                          : Group 2 - Query string (starts with ?, no space, quote)
    text = re.sub(
        r"([a-zA-Z][a-zA-Z0-9+.-]*://[^\s'\"?]+)(\?[^\s'\"]*)", r"\1?***", text
    )

    return text
