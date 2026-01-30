"""Shared BDD steps and fixtures."""

from pathlib import Path

import pytest
from pytest_bdd import given


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Fixture to provide a path to a clean DuckDB database."""
    db_path = tmp_path / "baliza_test.duckdb"
    if db_path.exists():
        db_path.unlink()
    return db_path

@given("a clean local data store", target_fixture="db_path")
def clean_local_data_store(db_path: Path) -> Path:
    """Ensure we start with a fresh database."""
    return db_path
