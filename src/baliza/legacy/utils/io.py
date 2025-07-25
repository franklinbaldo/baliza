"""I/O utilities for Baliza."""

from pathlib import Path


def load_sql_file(filename: str) -> str:
    """Load SQL file from src/baliza/sql/ directory."""
    sql_path = Path(__file__).parent.parent / "sql" / filename
    try:
        with open(sql_path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
