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
    """Validate that a URL does not point to a private/local network (SSRF protection).

    Args:
        url: The URL to validate.

    Returns:
        The validated URL.

    Raises:
        ValueError: If the URL is invalid or points to a forbidden network.
    """
    if os.environ.get("BALIZA_ALLOW_PRIVATE_NETWORKS") == "1":
        return url

    try:
        parsed = urlparse(url)
        if not parsed.hostname:
            raise ValueError("Invalid URL: Missing hostname")

        # Resolve hostname to IP(s)
        # using 0 for port/protocol/socktype to get all info
        addr_info = socket.getaddrinfo(parsed.hostname, None)

        for info in addr_info:
            # info[4][0] is the IP address
            ip_str = info[4][0]
            ip = ipaddress.ip_address(ip_str)

            if ip.is_private or ip.is_loopback or ip.is_link_local:
                raise ValueError(f"SSRF attempted: URL resolves to restricted IP {ip}")

    except (ValueError, socket.gaierror) as e:
        raise ValueError(f"Invalid URL or resolution failed: {e}") from e

    return url
