import json
from datetime import datetime
from pathlib import Path
from typing import Any

import ibis
import structlog

logger = structlog.get_logger()


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
            # DDL for schema still uses raw_sql as per Ibis standard for DuckDB
            self.con.raw_sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

            # Create quarantine table if it's the state schema
            if schema_name == "baliza_state":
                # Explicit Ibis schema definition
                schema = ibis.schema(
                    {
                        "resource": "string",
                        "extraction_date": "timestamp",
                        "error": "string",
                        "raw_json": "string",
                    }
                )

                tables = self.con.list_tables(database=schema_name)
                if "quarantine" not in tables:
                    self.con.create_table("quarantine", schema=schema, database=schema_name)
        except Exception as e:
            logger.error("schema_init_failed", schema=schema_name, error=str(e))
            raise RuntimeError(f"Could not initialize schema {schema_name}") from e

    def ingest_jsonl(self, jsonl_path: Path, table_name: str, schema: str = "main") -> int:
        """Ingest a JSONL file into a table using Ibis."""
        if not jsonl_path.exists():
            return 0

        # Read JSONL using Ibis/DuckDB
        t = self.con.read_json(str(jsonl_path))

        # Schema-qualified table check
        tables = self.con.list_tables(database=schema)
        if table_name in tables:
            self.con.insert(table_name, t, database=schema)
        else:
            self.con.create_table(table_name, t, database=schema)

        return t.count().execute()

    def get_table(self, table_name: str, schema: str = "main"):
        """Return an Ibis table expression."""
        return self.con.table(table_name, database=schema)

    def quarantine_record(
        self, resource: str, extraction_date: datetime, error: str, raw: dict[str, Any]
    ):
        """Save a failed record to the quarantine table for the current session via native Ibis."""
        row = {
            "resource": [resource],
            "extraction_date": [extraction_date],
            "error": [error],
            "raw_json": [json.dumps(raw)],
        }
        # Use memtable and insert (Native Ibis path)
        t = ibis.memtable(row)
        self.con.insert("quarantine", t, database="baliza_state")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close the connection to prevent leaks."""
        try:
            if hasattr(self, "con"):
                self.con.disconnect()
        except Exception:
            pass
