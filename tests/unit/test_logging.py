import json
import logging
from unittest.mock import patch
import structlog
import pytest

from baliza.logging import configure_logging

def test_configure_logging_console(monkeypatch):
    monkeypatch.setenv("BALIZA_LOG_FORMAT", "console")
    configure_logging()

    logger = structlog.get_logger()
    assert logger is not None
    # We just ensure it doesn't crash
    logger.info("Test message console")

def test_configure_logging_json(monkeypatch, capsys):
    monkeypatch.setenv("BALIZA_LOG_FORMAT", "json")
    configure_logging()

    logger = structlog.get_logger()
    logger.info("test_event", custom_key="custom_value")

    captured = capsys.readouterr()
    output_lines = [line for line in captured.out.split('\n') if line]

    # Check that at least one output line is valid JSON with expected keys
    found_valid_json = False
    for line in output_lines:
        try:
            log_entry = json.loads(line)
            if "event" in log_entry and log_entry["event"] == "test_event":
                assert "timestamp" in log_entry
                assert "level" in log_entry
                assert log_entry["level"] == "info"
                assert log_entry["custom_key"] == "custom_value"
                found_valid_json = True
                break
        except json.JSONDecodeError:
            continue

    assert found_valid_json, f"Could not find valid JSON log output in {output_lines}"
