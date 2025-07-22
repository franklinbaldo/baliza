"""E2E Test Suite for BALIZA CLI Interface

Tests the command-line interface against real systems.
Validates user workflows from CLI perspective.
"""

import subprocess
from pathlib import Path

import pytest
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

from baliza.pncp_writer import BALIZA_DB_PATH


@pytest.mark.e2e
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=1, max=30),
    reraise=True,
)
def test_e2e_cli_help():
    """E2E test: Verify CLI help commands work correctly"""
    # Test main help
    result = subprocess.run(
        ["uv", "run", "baliza", "--help"],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )

    assert result.returncode == 0, f"CLI help failed: {result.stderr}"
    assert "extract" in result.stdout, "Extract command not found in help"


@pytest.mark.e2e
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=1, max=30),
    reraise=True,
)
def test_e2e_cli_extract_help():
    """E2E test: Verify extract command help works correctly"""
    result = subprocess.run(
        ["uv", "run", "baliza", "extract", "--help"],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )

    assert result.returncode == 0, f"Extract help failed: {result.stderr}"
    assert "--concurrency" in result.stdout, "Concurrency option not found"


@pytest.mark.e2e
@pytest.mark.slow
@retry(
    stop=stop_after_attempt(2),
    wait=wait_exponential_jitter(initial=2, max=60),
    reraise=True,
)
def test_e2e_cli_extract_single_day():
    """E2E test: Verify CLI extract command works for single day"""

    result = subprocess.run(
        [
            "uv",
            "run",
            "baliza",
            "extract",
            "--month",
            "2024-01",
            "--concurrency",
            "1",
            "--force-db",
        ],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )

    # CLI should complete successfully
    assert result.returncode == 0, f"Extract command failed: {result.stderr}"

    # Should show progress information
    assert "Extract" in result.stdout or "data" in result.stdout, (
        "Expected progress output not found"
    )


@pytest.mark.e2e
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=1, max=30),
    reraise=True,
)
def test_e2e_cli_stats():
    """E2E test: Verify CLI stats command works"""
    # Only run if database exists
    if BALIZA_DB_PATH.exists():
        result = subprocess.run(
            ["uv", "run", "baliza", "stats"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent,
        )

        assert result.returncode == 0, f"Stats command failed: {result.stderr}"

        # Should show some statistics
        assert "Total" in result.stdout or "Records" in result.stdout, (
            "Expected stats output not found"
        )
    else:
        pytest.skip("Database file not found - run extraction first")


@pytest.mark.e2e
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=1, max=30),
    reraise=True,
)
def test_e2e_cli_mcp_help():
    """E2E test: Verify MCP command help works"""
    result = subprocess.run(
        ["uv", "run", "baliza", "mcp", "--help"],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )

    assert result.returncode == 0, f"MCP help failed: {result.stderr}"
    assert "MCP" in result.stdout or "server" in result.stdout, (
        "Expected MCP help content not found"
    )


@pytest.mark.e2e
def test_e2e_cli_invalid_concurrency_format():
    """E2E test: Verify CLI properly handles invalid concurrency formats"""
    result = subprocess.run(
        ["uv", "run", "baliza", "extract", "--concurrency", "invalid-concurrency"],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )

    # Should fail with proper error message
    assert result.returncode != 0, "CLI should reject invalid concurrency"
    assert (
        "concurrency" in result.stderr.lower() or "invalid" in result.stderr.lower()
    ), "Expected concurrency validation error not found"


@pytest.mark.e2e
def test_e2e_cli_concurrency_validation():
    """E2E test: Verify CLI properly validates concurrency parameter"""
    result = subprocess.run(
        [
            "uv",
            "run",
            "baliza",
            "extract",
            "--month",
            "2024-01",
            "--concurrency",
            "0",  # Invalid concurrency
        ],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )

    # Should fail with proper error message
    assert result.returncode != 0, "CLI should reject invalid concurrency"
    assert (
        "concurrency" in result.stderr.lower() or "invalid" in result.stderr.lower()
    ), "Expected concurrency validation error not found"
