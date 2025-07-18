"""Content utilities for BALIZA database optimization.

This module provides utilities for content deduplication using UUIDv5-based
content identification and SHA-256 hashing for integrity verification.
"""

import hashlib
import uuid

# BALIZA namespace UUID for UUIDv5 generation
# Generated once for the project - all content IDs will use this namespace
BALIZA_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


def normalize_content(content: str) -> str:
    """Normalize content for consistent hashing.

    Args:
        content: Raw response content string

    Returns:
        Normalized content string for hashing
    """
    if not content:
        return ""

    # Normalize whitespace and encoding for consistent hashing
    return content.strip()


def generate_content_hash(content: str) -> str:
    """Generate SHA-256 hash of normalized content.

    Args:
        content: Response content string

    Returns:
        Hexadecimal SHA-256 hash string
    """
    normalized = normalize_content(content)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def generate_content_id(content: str) -> str:
    """Generate deterministic UUIDv5 based on content.

    This creates a deterministic UUID that will always be the same
    for the same content, enabling efficient deduplication.

    Args:
        content: Response content string

    Returns:
        UUIDv5 string based on content hash
    """
    content_hash = generate_content_hash(content)
    return str(uuid.uuid5(BALIZA_NAMESPACE, content_hash))


def analyze_content(content: str) -> tuple[str, str, int]:
    """Analyze content and return all relevant metadata.

    Args:
        content: Response content string

    Returns:
        Tuple of (content_id, content_hash, content_size)
    """
    normalized = normalize_content(content)
    content_hash = generate_content_hash(content)
    content_id = str(uuid.uuid5(BALIZA_NAMESPACE, content_hash))
    content_size = len(normalized.encode("utf-8"))

    return content_id, content_hash, content_size


def verify_content_integrity(content: str, expected_hash: str) -> bool:
    """Verify content integrity against expected hash.

    Args:
        content: Content to verify
        expected_hash: Expected SHA-256 hash

    Returns:
        True if content matches expected hash
    """
    actual_hash = generate_content_hash(content)
    return actual_hash == expected_hash


def is_empty_response(content: str | None) -> bool:
    """Check if response content is effectively empty.

    Args:
        content: Response content to check

    Returns:
        True if content is None, empty, or only whitespace
    """
    if not content:
        return True

    normalized = normalize_content(content)
    return len(normalized) == 0


def estimate_deduplication_savings(
    unique_content_size: int, total_content_size: int
) -> dict:
    """Estimate storage savings from content deduplication.

    Args:
        unique_content_size: Size of unique content after deduplication
        total_content_size: Size of all content before deduplication

    Returns:
        Dictionary with savings metrics
    """
    if total_content_size == 0:
        return {
            "savings_bytes": 0,
            "savings_percentage": 0.0,
            "compression_ratio": 1.0,
            "duplicate_content_size": 0,
        }

    savings_bytes = total_content_size - unique_content_size
    savings_percentage = (savings_bytes / total_content_size) * 100
    compression_ratio = unique_content_size / total_content_size

    return {
        "savings_bytes": savings_bytes,
        "savings_percentage": round(savings_percentage, 2),
        "compression_ratio": round(compression_ratio, 3),
        "duplicate_content_size": savings_bytes,
    }


# Utility constants for database operations
CONTENT_ID_LENGTH = 36  # UUID string length
CONTENT_HASH_LENGTH = 64  # SHA-256 hex string length


if __name__ == "__main__":
    # Example usage and testing
    test_content = '{"data": [{"id": 1, "name": "test"}], "totalRegistros": 1}'

    content_id, content_hash, content_size = analyze_content(test_content)

    print("Content Analysis:")
    print(f"  Content ID (UUIDv5): {content_id}")
    print(f"  Content Hash (SHA-256): {content_hash}")
    print(f"  Content Size: {content_size} bytes")
    print(f"  Integrity Check: {verify_content_integrity(test_content, content_hash)}")

    # Test deduplication
    duplicate_content = test_content  # Same content
    duplicate_id, duplicate_hash, _ = analyze_content(duplicate_content)

    print("\nDeduplication Test:")
    print(f"  Same Content IDs: {content_id == duplicate_id}")
    print(f"  Same Content Hashes: {content_hash == duplicate_hash}")
