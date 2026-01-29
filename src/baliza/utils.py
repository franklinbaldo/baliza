"""Validation utilities."""

import re
import socket
import os
import ipaddress
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
    """Validate that a URL is safe to use (http/https, no private IPs).

    Args:
        url: The URL to validate.

    Returns:
        The validated URL.

    Raises:
        ValueError: If the URL is invalid or points to a private network (SSRF).
    """
    if not url:
        raise ValueError("URL cannot be empty")

    try:
        parsed = urlparse(url)
    except Exception as e:
        raise ValueError(f"Invalid URL: {e}") from e

    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"Invalid scheme: '{parsed.scheme}'. Must be http or https.")

    if not parsed.hostname:
        raise ValueError("URL must have a hostname")

    # Allow bypassing private network check via env var (useful for testing/internal use)
    if os.environ.get("BALIZA_ALLOW_PRIVATE_NETWORKS") == "1":
        return url

    try:
        # Resolve hostname to IP
        # Note: This is a basic check. DNS rebinding attacks are still possible
        # if the time-of-check vs time-of-use is not handled, but httpx doesn't
        # make it easy to separate DNS resolution.
        # This provides a basic layer of protection against accidental usage.
        ip_str = socket.gethostbyname(parsed.hostname)
        ip = ipaddress.ip_address(ip_str)

        if ip.is_private or ip.is_loopback or ip.is_link_local:
            raise ValueError(f"URL resolves to private IP: {ip_str}. Access denied.")

    except socket.gaierror:
        # Fail open or closed?
        # If we can't resolve, we can't check IP.
        # But if we can't resolve, httpx will likely fail too.
        # Let's fail safe (closed) for security.
        # But wait, transient DNS failures could block valid traffic.
        # However, for a CLI tool, immediate feedback is better.
        raise ValueError(f"Could not resolve hostname: {parsed.hostname}")

    return url
