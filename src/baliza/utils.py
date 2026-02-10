"""Validation utilities."""

import ipaddress
import os
import re
import socket
from urllib.parse import urlparse, urlunparse


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


def secure_url_connection_params(url: str) -> tuple[str, dict[str, str]]:
    """Resolve URL to an IP address (for HTTP) to prevent DNS rebinding.

    For HTTP: Resolves hostname, validates IP, and returns (url_with_ip, {'Host': hostname}).
    For HTTPS: Validates IP, but returns (original_url, {}) to preserve SNI/Cert validation.

    If bypass is enabled via env var, returns (url, {}).

    Args:
        url: The URL to secure.

    Returns:
        Tuple of (safe_url, headers).

    Raises:
        ValueError: If validation fails.
    """
    if not url:
        raise ValueError("URL cannot be empty.")

    try:
        parsed = urlparse(url)
    except Exception as e:
        raise ValueError(f"Invalid URL format: {e}") from e

    # Basic validation
    if parsed.scheme not in ("http", "https"):
        raise ValueError("URL must use http or https scheme.")

    if not parsed.hostname:
        raise ValueError("URL must have a hostname.")

    # Allow bypass via env var
    if os.getenv("BALIZA_ALLOW_PRIVATE_NETWORKS") == "1":
        return url, {}

    hostname = parsed.hostname
    port = parsed.port

    # Handle IPv6 literals in hostname (remove brackets)
    is_ipv6_literal = False
    if hostname.startswith("[") and hostname.endswith("]"):
        hostname = hostname[1:-1]
        is_ipv6_literal = True

    try:
        # Resolve hostname to IP addresses
        addr_info = socket.getaddrinfo(hostname, None)
    except socket.gaierror as e:
        raise ValueError(f"Could not resolve hostname '{hostname}': {e}") from e

    resolved_ip = None

    for _, _, _, _, sockaddr in addr_info:
        ip_str = sockaddr[0]
        try:
            ip_obj = ipaddress.ip_address(ip_str)
        except ValueError:
            continue

        # Ensure the IP is globally reachable and not multicast
        if not ip_obj.is_global or ip_obj.is_multicast:
            raise ValueError(f"URL resolves to non-global or multicast IP: {ip_str}")

        # Pick the first valid IP
        if resolved_ip is None:
            resolved_ip = ip_obj

    if resolved_ip is None:
        raise ValueError(f"Could not resolve valid IP for {hostname}")

    # If HTTPS, return original URL (SSL handles validation)
    if parsed.scheme == "https":
        return url, {}

    # If HTTP, rewrite URL to use IP
    new_netloc = str(resolved_ip)
    if resolved_ip.version == 6:
        new_netloc = f"[{new_netloc}]"

    if port:
        new_netloc = f"{new_netloc}:{port}"

    # Construct new URL
    # urlunparse expects: scheme, netloc, path, params, query, fragment
    new_url = urlunparse((
        parsed.scheme,
        new_netloc,
        parsed.path,
        parsed.params,
        parsed.query,
        parsed.fragment
    ))

    # Set Host header
    # Note: parsed.hostname does not include port, but Host header usually should if non-standard?
    # Actually, httpx sets Host header automatically from URL.
    # If we override it, we should provide the original netloc (hostname:port).
    # parsed.netloc includes user:pass@host:port. We want host:port.

    # Use parsed.netloc but strip user:pass if present?
    # If user:pass is present, it's safer to keep it in URL or header?
    # If we change netloc to IP, user info might be lost if we don't handle it.

    # Let's handle user/pass
    user_info = ""
    if parsed.username:
        user_info = f"{parsed.username}"
        if parsed.password:
            user_info += f":{parsed.password}"
        user_info += "@"

    final_netloc = f"{user_info}{new_netloc}"

    # Re-construct URL with user info if any
    new_url = urlunparse((
        parsed.scheme,
        final_netloc,
        parsed.path,
        parsed.params,
        parsed.query,
        parsed.fragment
    ))

    # Host header should be the original hostname (and port if non-standard)
    # If user provided user:pass@host:port, Host header should be host:port
    original_host_header = parsed.hostname
    if port:
        original_host_header = f"{original_host_header}:{port}"

    return new_url, {"Host": original_host_header}


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
