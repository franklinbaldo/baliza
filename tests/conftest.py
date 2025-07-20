"""
Test configuration for E2E tests only.
Aligned with ADR: E2E tests are the only source of truth.
"""

import asyncio
import os
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
    config.addinivalue_line("markers", "asyncio: marks tests as asyncio-based tests")


@pytest.fixture(scope="session")
def project_root():
    """Get the project root directory."""
    return Path(__file__).parent.parent


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def test_data_dir(project_root):
    """Get the test data directory."""
    return project_root / "data"


@pytest.fixture(scope="function")
def clean_test_environment():
    """Ensure clean test environment (but preserve data for E2E tests)."""
    # E2E tests need to preserve data between runs for resumability testing
    # This fixture can be used for tests that need isolation
    yield
    # Cleanup is optional for E2E tests - we want to preserve state


@pytest.fixture(scope="session")
def skip_if_no_database():
    """Skip tests if database doesn't exist."""
    from baliza.pncp_writer import BALIZA_DB_PATH

    if not BALIZA_DB_PATH.exists():
        pytest.skip("Database file not found - run extraction first")




# Set environment variables for testing
@pytest.fixture(scope="session", autouse=True)
def configure_test_environment():
    """Configure environment for E2E testing."""
    # Set test-specific environment variables
    os.environ["BALIZA_TEST_MODE"] = "true"

    # Ensure we're using the test data directory
    test_data_dir = Path.cwd() / "data"
    test_data_dir.mkdir(exist_ok=True)

    yield

    # Cleanup environment
    os.environ.pop("BALIZA_TEST_MODE", None)
