"""Bounded IA publication retries; never retry credentials or invalid requests."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import internetarchive as ia
import requests
import structlog

logger = structlog.get_logger()


class PublicationError(RuntimeError):
    """A publication was rejected or its acknowledgement is missing."""


def rejection_kind(error: Exception) -> str:
    response = getattr(error, "response", None)
    status = getattr(response, "status_code", None)
    message = (str(error) + " " + str(getattr(response, "text", ""))).lower()
    # IA sometimes uses a non-429 response for its global task queue limit.
    if "total_tasks_queued" in message and "global_limit" in message:
        return "capacity"
    if status in (408, 429, 500, 502, 503, 504):
        return "temporary"
    if isinstance(error, (requests.Timeout, requests.ConnectionError)):
        return "temporary"
    return "permanent"


def _retry_after(error: Exception) -> float:
    response = getattr(error, "response", None)
    value = getattr(response, "headers", {}).get("Retry-After", "")
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            return max(0.0, (parsedate_to_datetime(value) - datetime.now(UTC)).total_seconds())
        except (ValueError, TypeError, OverflowError):
            return 0.0


def upload_with_retry(
    identifier: str,
    *,
    max_attempts: int = 6,
    max_wait_seconds: float = 900,
    **kwargs: Any,
) -> None:
    """Retry at most six PUT attempts and 15 minutes of cumulative waiting.

    Paths and remote names remain unchanged across attempts. IA's internal
    retries are disabled so they cannot multiply this budget. HTTP operations
    retain the SDK's 120s timeout. No credentials or response bodies are logged.
    Retry-After is a lower bound: fail for later resumption if it exceeds budget.
    """
    if max_attempts < 1 or max_wait_seconds < 0:
        raise ValueError("Invalid publication retry limits")
    kwargs["retries"] = 0
    waited = 0.0
    for attempt in range(1, max_attempts + 1):
        try:
            responses = ia.upload(identifier, **kwargs)
            if not responses:
                raise PublicationError("IA returned no upload acknowledgement")
            for response in responses:
                if not isinstance(response, requests.Response):
                    raise PublicationError("IA returned an invalid upload acknowledgement")
                response.raise_for_status()
                if not 200 <= response.status_code < 300:
                    raise PublicationError("IA returned an invalid upload acknowledgement")
            return
        except Exception as error:
            kind = rejection_kind(error)
            if kind == "permanent":
                raise PublicationError("Permanent IA publication failure") from error
            delay = max(
                _retry_after(error),
                min(300, (60 if kind == "capacity" else 30) * 2 ** (attempt - 1)),
            )
            if attempt == max_attempts or waited + delay > max_wait_seconds:
                raise PublicationError(f"IA {kind} retry budget exhausted; resume later") from error
            logger.warning(
                "ia_publication_retry",
                item=identifier,
                kind=kind,
                attempt=attempt,
                delay_seconds=delay,
            )
            time.sleep(delay)
            waited += delay
