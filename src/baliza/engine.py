import json
from datetime import datetime
from pathlib import Path
from typing import Any

import ibis
import structlog

logger = structlog.get_logger()


class BalizaEngine:
    """Ibis-based backend for DuckDB. Stateless by default (in-memory)."""

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
            # This is acceptable as it's part of connection setup, but we'll try to reach for con.create_database if needed
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

    def upsert_rows(
        self,
        data: list[dict[str, Any]],
        table_name: str,
        schema: str = "main",
        pk: str = "numeroControlePNCP",
    ) -> int:
        """Upsert rows using pure Ibis logic (Union + Overwrite)."""
        if not data:
            return 0

        # Create a memtable from the new data
        t_new = ibis.memtable(data)

        # Ensure no NULL typed columns in the memtable for DuckDB compatibility
        new_schema = {}
        for name, dtype in t_new.schema().items():
            dtype_str = str(dtype).lower()
            if dtype_str in ("null", "nulltype", "null-type"):
                new_schema[name] = "string"
            elif "timestamp" in dtype_str:
                new_schema[name] = "timestamp"
            else:
                new_schema[name] = dtype
        t_new = t_new.cast(new_schema)

        tables = self.con.list_tables(database=schema)
        if table_name in tables:
            # PURE IBIS UPSERT:
            # 1. Get existing table
            t_existing = self.con.table(table_name, database=schema)
            
            # 2. ALIGN SCHEMAS: Force new data to match existing DB schema exactly 
            # (resolves DuckDB timestamp vs timestamp(6) conflicts)
            try:
                t_new = t_new.cast(t_existing.schema())
            except Exception:
                # Fallback if Ibis cast fails (e.g. new columns added in code)
                pass

            # 3. Filter out rows that are in the new batch (based on PK)
            # 4. Union with new batch
            t_combined = ibis.union(t_existing.filter(~t_existing[pk].isin(t_new[pk])), t_new)
            # 5. Overwrite table
            self.con.create_table(table_name, t_combined, database=schema, overwrite=True)
        else:
            # Simple creation
            self.con.create_table(table_name, t_new, database=schema)

        return len(data)

    def ingest_jsonl(self, json_path: Path, table_name: str, schema: str = "main") -> int:
        """DEPRECATED: Use upsert_rows instead. Ingest a JSONL file."""
        if not Path(json_path).exists():
            return 0
        
        # Load as Ibis table
        t = self.con.read_json(str(json_path))
        
        # Call upsert_rows to ensure idempotency even for JSONL ingestion
        # (This is more consistent than just calling self.con.insert)
        data = t.execute().to_dict("records")
        return self.upsert_rows(data, table_name, schema=schema)

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
