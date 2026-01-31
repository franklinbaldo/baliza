"""Validation utilities."""

import ipaddress
import os
import re
import socket
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
        raise ValueError(
            f"Invalid identifier: '{name}'. Must be alphanumeric with underscores only."
        )
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
        raise ValueError(
            f"Invalid resource path: '{path}'. Must be alphanumeric with '-', '_', or '/'."
        )

    if ".." in path:
        raise ValueError(
            f"Invalid resource path: '{path}'. Path traversal ('..') is not allowed."
        )

    return path


def validate_url(url: str) -> str:
    """Validate that a URL is safe and does not point to a private network.

    Prevents SSRF attacks by resolving the hostname and checking if the IP is private.
    Can be bypassed by setting BALIZA_ALLOW_PRIVATE_NETWORKS=1 environment variable.

    Args:
        url: The URL to validate.

    Returns:
        The validated URL.

    Raises:
        ValueError: If the URL is invalid or points to a private network.
    """
    if os.environ.get("BALIZA_ALLOW_PRIVATE_NETWORKS") == "1":
        return url

    try:
        parsed = urlparse(url)
    except Exception as e:
        raise ValueError(f"Invalid URL format: {e}") from e

    if parsed.scheme not in ("http", "https"):
        raise ValueError(
            f"Invalid URL scheme: '{parsed.scheme}'. Must be http or https."
        )

    if not parsed.hostname:
        raise ValueError("Invalid URL: missing hostname.")

    hostname = parsed.hostname
    # Remove brackets from IPv6 literals for resolution
    if hostname.startswith("[") and hostname.endswith("]"):
        hostname = hostname[1:-1]

    try:
        # Resolve hostname to IP(s)
        # Use getaddrinfo to handle both IPv4 and IPv6
        addr_info = socket.getaddrinfo(hostname, None)
    except socket.gaierror as e:
        raise ValueError(f"Could not resolve hostname '{hostname}': {e}") from e

    for _, _, _, _, sockaddr in addr_info:
        ip_str = sockaddr[0]
        try:
            ip = ipaddress.ip_address(ip_str)
        except ValueError:
            continue

        if (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_reserved
            or (ip.is_unspecified)
        ):
            raise ValueError(
                f"Internal/Private network access denied: {hostname} resolves to {ip_str}"
            )

    return url
