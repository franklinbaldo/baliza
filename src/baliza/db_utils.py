"""Database utility functions for the baliza package."""

import logging
from pathlib import Path

import duckdb
from rich.console import Console

logger = logging.getLogger(__name__)
console = Console(force_terminal=True, legacy_windows=False, stderr=False)

def connect_db(path: str, force: bool = False) -> duckdb.DuckDBPyConnection:
    """Connect to DuckDB with UTF-8 error handling."""
    try:
        if force:
            # Force mode: try read-only connection first
            try:
                return duckdb.connect(path, read_only=True)
            except duckdb.Error:
                # If read-only fails, try regular connection
                pass

        return duckdb.connect(path)
    except duckdb.Error as exc:
        if force and "lock" in str(exc).lower():
            console.print("⚠️ [yellow]Database locked by another process, trying read-only access[/yellow]")
            try:
                return duckdb.connect(path, read_only=True)
            except duckdb.Error as read_exc:
                error_msg = f"Database connection failed even in read-only mode: {read_exc}"
                raise RuntimeError(error_msg) from read_exc
        else:
            # DuckDB error - preserve original exception with clean message
            error_msg = f"Database connection failed: {exc}"
            raise RuntimeError(error_msg) from exc

def execute_sql_from_file(conn: duckdb.DuckDBPyConnection, filepath: Path):
    """Execute SQL commands from a file."""
    with open(filepath, 'r') as f:
        sql = f.read()
        conn.execute(sql)
