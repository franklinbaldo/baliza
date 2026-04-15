"""Structured logging configuration for Baliza.

Uses structlog for machine-parseable, context-bound logging.
Set BALIZA_LOG_FORMAT=json for JSON output (useful in CI).
"""

from __future__ import annotations

import os

import structlog


def configure_logging() -> None:
    """Configure structlog with console or JSON renderer.

    Reads BALIZA_LOG_FORMAT env var:
    - "json": JSONRenderer for CI/log aggregation
    - "console" (default): ConsoleRenderer for human-readable output
    """
    log_format = os.environ.get("BALIZA_LOG_FORMAT", "console")

    if log_format == "json":
        renderer: structlog.types.Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(0),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
