import json
from datetime import datetime
from pathlib import Path
from typing import Any

import ibis


class BalizaEngine:
    """Ibis-based backend for DuckDB. Statless by default (in-memory)."""

    def __init__(self, db_path: Path | str | None = None, connection: Any = None):
        """Initialize engine. 
        
        Args:
            db_path: Path to DuckDB file. If None or ":memory:", uses in-memory.
            connection: Optional existing Ibis connection to reuse.
        """
        if connection:
            self.con = connection
            self.path = getattr(connection, "path", ":memory:")
        else:
            # Handle the magic string ":memory:" vs actual Path objects
            if not db_path or str(db_path) == ":memory:":
                self.path = ":memory:"
            else:
                self.path = str(db_path)
            
            self.con = ibis.duckdb.connect(self.path)
        
        self._ensure_schema("main")
        self._ensure_schema("baliza_state")

    def _ensure_schema(self, schema_name: str = "baliza_state"):
        """Create a schema and necessary state tables if they don't exist."""
        try:
            self.con.raw_sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            
            # Create quarantine table if it's the state schema
            if schema_name == "baliza_state":
                self.con.raw_sql(
                    "CREATE TABLE IF NOT EXISTS baliza_state.quarantine ("
                    "  resource VARCHAR,"
                    "  extraction_date TIMESTAMP,"
                    "  error VARCHAR,"
                    "  raw_json VARCHAR"
                    ")"
                )
        except Exception:
            pass

    def ingest_jsonl(self, jsonl_path: Path, table_name: str, schema: str = "main") -> int:
        """Ingest a JSONL file into a table using Ibis."""
        if not jsonl_path.exists():
            return 0

        # Read JSONL using Ibis/DuckDB
        t = self.con.read_json(str(jsonl_path))
        
        # Simple table existence check in Ibis
        # DuckDB/Ibis list_tables doesn't always take 'schema' in all versions
        if table_name in self.con.list_tables():
            self.con.insert(table_name, t, database=schema)
        else:
            self.con.create_table(table_name, t, database=schema)
            
        return t.count().execute()

    def get_table(self, table_name: str, schema: str = "main"):
        """Return an Ibis table expression."""
        return self.con.table(table_name, database=schema)

    def quarantine_record(self, resource: str, extraction_date: datetime, error: str, raw: dict[str, Any]):
        """Save a failed record to the quarantine table for the current session."""
        # Access the underlying DuckDB connection for parameterized execution
        self.con.con.execute(
            "INSERT INTO baliza_state.quarantine (resource, extraction_date, error, raw_json) "
            "VALUES (?, ?, ?, ?)",
            [resource, extraction_date, error, json.dumps(raw)],
        )

    def execute_sql(self, sql: str, params: list[Any] | None = None):
        """Execute raw SQL with parameters."""
        if params:
            self.con.con.execute(sql, params)
        else:
            self.con.raw_sql(sql)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # We don't close the connection here if it's shared, 
        # but for standalone use it's fine.
        pass
