"""Utility functions for the baliza package."""

import json
import logging
from typing import Any

import orjson

logger = logging.getLogger(__name__)


def parse_json_robust(content: str) -> Any:
    """Parse JSON with orjson (fast) and fallback to stdlib json for edge cases."""
    try:
        return orjson.loads(content)
    except orjson.JSONDecodeError as e:
        logger.warning(
            f"orjson failed to parse JSON, falling back to standard json: {e}"
        )
        try:
            return json.loads(content)
        except json.JSONDecodeError as e:
            logger.exception(
                f"Failed to parse JSON with both orjson and standard json: {e}"
            )
            raise
