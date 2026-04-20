"""Validation utilities."""

from __future__ import annotations

import ipaddress
import os
import re
import socket
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import ParseResult, urlparse, urlunparse

import pyarrow as pa
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from ipaddress import IPv4Address, IPv6Address


# Tuned for DuckDB-WASM HTTP range reads: small enough that a point
# lookup touches ~0.5–1 MB compressed, matches DuckDB's default.
PARQUET_ROW_GROUP_SIZE = 8192

# ZSTD L9 is ~15% smaller than L3; write cost is paid once nightly.
PARQUET_COMPRESSION_LEVEL = 9

# DuckDB COPY TO options string for WASM-optimized Parquet:
# - write_bloom_filter: writes Bloom filters on all eligible (dict-encoded)
#   columns — enables row-group skipping for equality filters on PK, cnpj,
#   ni_fornecedor, codigo_ibge without per-column configuration.
# - bloom_filter_false_positive_ratio 0.01: ~1% FPP, ~1-2% file overhead.
# - row_group_size 8192: point lookups touch ~0.5–1 MB compressed.
# - compression_level 9: write-once-read-many archive pattern.
DUCKDB_PARQUET_COPY_OPTIONS = (
    f"FORMAT PARQUET, "
    f"COMPRESSION ZSTD, COMPRESSION_LEVEL {PARQUET_COMPRESSION_LEVEL}, "
    f"ROW_GROUP_SIZE {PARQUET_ROW_GROUP_SIZE}, "
    f"write_bloom_filter true, "
    f"bloom_filter_false_positive_ratio 0.01"
)


def write_optimized_parquet(
    reader: pa.RecordBatchReader | pa.Table,
    path: Path,
    schema: pa.Schema,
    table_name: str,
    row_group_size: int = PARQUET_ROW_GROUP_SIZE,
) -> tuple[int, int]:
    """Write a PyArrow table/reader as Parquet tuned for DuckDB-WASM range reads.

    Used by small per-day files (daily_exporter) where bloom filters are less
    critical. For WASM-hit monthly/annual/shard files the DuckDB writer path
    is preferred — it produces bloom filters natively.

    Options applied:
    - ZSTD compression level 9
    - Row group size 8192 (point lookups touch one row group)
    - Page index enabled (per-page min/max skipping)
    - Parquet v2.6

    Args:
        reader: Arrow Table or RecordBatchReader to write.
        path: Destination file path.
        schema: Target schema (used for casting each batch).
        table_name: Logical table name (unused — kept for callsite symmetry with
            the DuckDB writer path).
        row_group_size: Rows per row group.

    Returns:
        (file_size_bytes, row_count).
    """
    del table_name

    writer_kwargs: dict = {
        "compression": "zstd",
        "compression_level": PARQUET_COMPRESSION_LEVEL,
        "write_statistics": True,
        "version": "2.6",
        "write_page_index": True,
    }

    row_count = 0
    with pq.ParquetWriter(path, schema=schema, **writer_kwargs) as writer:
        if isinstance(reader, pa.Table):
            try:
                table = reader.cast(schema)
            except pa.ArrowInvalid:
                table = reader
            writer.write_table(table, row_group_size=row_group_size)
            row_count = table.num_rows
        else:
            for batch in reader:
                try:
                    t = pa.Table.from_batches([batch]).cast(schema)
                except pa.ArrowInvalid:
                    t = pa.Table.from_batches([batch])
                writer.write_table(t, row_group_size=row_group_size)
                row_count += t.num_rows

    return path.stat().st_size, row_count


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

    # Ensure port is safe (block privileged ports except 80/443)
    # This prevents SSRF port scanning of system services (SSH, SMTP, etc.)
    if parsed.port and parsed.port < 1024 and parsed.port not in (80, 443):
        raise ValueError(f"URL uses unsafe port: {parsed.port}")

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


def _resolve_safe_ip(hostname: str) -> IPv4Address | IPv6Address:
    """Resolve a hostname to a safe, global IP address.

    Args:
        hostname: The hostname to resolve.

    Returns:
        First valid global IP address found.

    Raises:
        ValueError: If resolution fails or all IPs are unsafe (private/multicast).
    """
    # Handle IPv6 literals in hostname (remove brackets)
    if hostname.startswith("[") and hostname.endswith("]"):
        hostname = hostname[1:-1]

    try:
        # Resolve hostname to IP addresses
        addr_info = socket.getaddrinfo(hostname, None)
    except socket.gaierror as e:
        raise ValueError(f"Could not resolve hostname '{hostname}': {e}") from e

    safe_ips = []
    for _, _, _, _, sockaddr in addr_info:
        ip_str = sockaddr[0]
        try:
            ip_obj = ipaddress.ip_address(ip_str)
        except ValueError:
            continue

        # Ensure the IP is globally reachable and not multicast
        if not ip_obj.is_global or ip_obj.is_multicast:
            raise ValueError(f"URL resolves to non-global or multicast IP: {ip_str}")

        safe_ips.append(ip_obj)

    if not safe_ips:
        raise ValueError(f"Could not resolve valid IP for {hostname}")

    return safe_ips[0]


def _rewrite_url_with_ip(
    parsed: ParseResult, ip_obj: IPv4Address | IPv6Address
) -> tuple[str, dict[str, str]]:
    """Rewrite a URL to use a specific IP address and return necessary headers.

    Args:
        parsed: The parsed original URL.
        ip_obj: The resolved safe IP address.

    Returns:
        Tuple of (new_url, headers) where headers includes the Host header.
    """
    new_netloc = str(ip_obj)
    if ip_obj.version == 6:
        new_netloc = f"[{new_netloc}]"

    if parsed.port:
        new_netloc = f"{new_netloc}:{parsed.port}"

    # Re-construct URL with user info if any
    user_info = ""
    if parsed.username:
        user_info = f"{parsed.username}"
        if parsed.password:
            user_info += f":{parsed.password}"
        user_info += "@"

    final_netloc = f"{user_info}{new_netloc}"

    new_url = urlunparse(
        (
            parsed.scheme,
            final_netloc,
            parsed.path,
            parsed.params,
            parsed.query,
            parsed.fragment,
        )
    )

    # Host header should be the original hostname (and port if non-standard)
    original_host_header = parsed.hostname
    if parsed.port:
        original_host_header = f"{original_host_header}:{parsed.port}"

    return new_url, {"Host": str(original_host_header)}


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

    # Ensure port is safe (block privileged ports except 80/443)
    if parsed.port and parsed.port < 1024 and parsed.port not in (80, 443):
        raise ValueError(f"URL uses unsafe port: {parsed.port}")

    # Resolve IP (this validates SSRF rules)
    resolved_ip = _resolve_safe_ip(parsed.hostname)

    # If HTTPS, return original URL (SSL handles validation)
    # We still resolved it above to ensure at least one valid IP exists (fail fast),
    # although technically race conditions could still happen with HTTPS if we don't pin the IP.
    # However, pinning IP for HTTPS breaks SNI in many clients unless custom transport is used.
    # Given the risk profile, HTTPS + Valid Cert is generally considered safe enough against Rebinding.
    if parsed.scheme == "https":
        return url, {}

    # If HTTP, rewrite URL to use IP
    return _rewrite_url_with_ip(parsed, resolved_ip)


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
        raise ValueError(
            f"Invalid identifier: '{name}'. Length {len(name)} exceeds limit of {max_length}."
        )

    if not re.match(r"^[a-zA-Z0-9_]+$", name):
        raise ValueError(
            f"Invalid identifier: '{name}'. Must be alphanumeric with underscores only."
        )
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
        raise ValueError(
            f"Invalid resource path: '{path}'. Length {len(path)} exceeds limit of {max_length}."
        )

    if not re.match(r"^[a-zA-Z0-9_\-/]+$", path):
        raise ValueError(
            f"Invalid resource path: '{path}'. Must be alphanumeric with '-', '_', or '/'."
        )

    if ".." in path:
        raise ValueError(f"Invalid resource path: '{path}'. Path traversal ('..') is not allowed.")

    return path



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
    text = re.sub(r"([a-zA-Z][a-zA-Z0-9+.-]*://)([^/@\s'\"]+)(@)", r"\1***\3", text)

    # 2. Scrub query parameters: scheme://path?query
    # ([a-zA-Z][a-zA-Z0-9+.-]*://[^\s'\"?]+) : Group 1 - Scheme + path (no space, quote, ?)
    # (\?[^\s'\"]*)                          : Group 2 - Query string (starts with ?, no space, quote)
    text = re.sub(r"([a-zA-Z][a-zA-Z0-9+.-]*://[^\s'\"?]+)(\?[^\s'\"]*)", r"\1?***", text)

    return text
