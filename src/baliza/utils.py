import hashlib
import json

# TODO: This utility module currently only contains a hashing function.
#       Consider if this module should be expanded to include other general-purpose
#       utility functions used across the project, or if it should remain focused
#       solely on hashing. If other utilities are added, consider renaming the file
#       to something more generic like `common_utils.py`. If only hashing-related
#       functions are present, consider renaming this file to `hashing_utils.py`.

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
