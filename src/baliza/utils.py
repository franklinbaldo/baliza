import hashlib
import json

def hash_sha256(data: dict) -> str:
    """Generates a SHA256 hash for a given dictionary.
    The dictionary is first serialized to a JSON string to ensure consistent hashing.
    """
    # TODO: Using SHA256 might be overkill for simple deduplication and could
    #       have performance implications on large datasets. Evaluate if a faster,
    #       non-cryptographic hash like xxHash would be sufficient for the use case
    #       of identifying unique records.
    # Ensure consistent serialization by sorting keys
    serialized_data = json.dumps(data, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(serialized_data.encode("utf-8")).hexdigest()
