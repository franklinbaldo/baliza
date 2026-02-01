from pathlib import Path
import pytest
from pytest_bdd import given

@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Create a clean DuckDB database."""
    db = tmp_path / "test.duckdb"
    if db.exists():
        db.unlink()
    return db

@given("a clean local data store", target_fixture="db_path")
def clean_local_data_store(db_path: Path) -> Path:
    """Create a clean local data store."""
    if db_path.exists():
        db_path.unlink()
    return db_path
