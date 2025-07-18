"""
Test configuration for E2E tests only.
Aligned with ADR: E2E tests are the only source of truth.
"""

import sys
from pathlib import Path

import pytest

# Add src to Python path for E2E tests
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def pytest_configure(config):
    """Configure pytest with E2E-focused markers."""
    config.addinivalue_line(
        "markers", "e2e: marks tests as end-to-end integration tests"
    )
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (may take several minutes)"
    )


@pytest.fixture(scope="session")
def project_root():
    """Get the project root directory."""
    return Path(__file__).parent.parent