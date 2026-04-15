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

    def upsert_rows(
        self, data: list[dict[str, Any]], table_name: str, schema: str = "main", pk: str = "numeroControlePNCP"
    ) -> int:
        """Upsert a list of dictionaries into a table using Ibis/DuckDB."""
        if not data:
            return 0

        # Create a memtable from the new data
        t_new = ibis.memtable(data)
        
        # Ensure no NULL typed columns (DuckDB hates them)
        new_schema = {}
        for name, dtype in t_new.schema().items():
            if str(dtype).lower() in ("null", "nulltype", "null-type"):
                new_schema[name] = "string"
            else:
                new_schema[name] = dtype
        t_new = t_new.cast(new_schema)

        # Check if table exists
        tables = self.con.list_tables(database=schema)
        if table_name not in tables:
            # Create table with Primary Key (requires raw_sql for PK constraint in DuckDB)
            # We first extract the schema from the memtable
            schema_def = t_new.schema()
            columns = []
            # Improved mapping for DuckDB
            type_map = {
                "string": "VARCHAR",
                "int64": "BIGINT",
                "float64": "DOUBLE",
                "timestamp": "TIMESTAMP",
                "timestamp('UTC')": "TIMESTAMP WITH TIME ZONE",
                "boolean": "BOOLEAN",
                "date": "DATE",
                "null": "VARCHAR",
                "nulltype": "VARCHAR",
            }
            
            for name, dtype in schema_def.items():
                dtype_str = str(dtype).lower()
                sql_type = type_map.get(dtype_str, "VARCHAR") # Fallback to VARCHAR
                
                if name == pk:
                    columns.append(f'"{name}" {sql_type} PRIMARY KEY')
                else:
                    columns.append(f'"{name}" {sql_type}')
            
            cols_str = ", ".join(columns)
            sql = f'CREATE TABLE {schema}."{table_name}" ({cols_str})'
            self.con.raw_sql(sql)
            self.con.insert(table_name, t_new, database=schema)
        else:
            # Use DuckDB's INSERT OR REPLACE for idempotency
            # This requires the table to have a PRIMARY KEY constraint
            temp_name = f"tmp_{table_name}_{datetime.now().strftime('%H%M%S')}"
            self.con.create_table(temp_name, t_new, temp=True)
            
            # native DuckDB UPSERT
            sql = f"""
                INSERT OR REPLACE INTO {schema}."{table_name}"
                SELECT * FROM {temp_name}
            """
            self.con.raw_sql(sql)
            self.con.drop_table(temp_name)

        return len(data)

    def ingest_jsonl(self, json_path: Path, table_name: str, schema: str = "main") -> int:
        """DEPRECATED: Use upsert_rows instead. Ingest a JSONL file."""
        if not Path(json_path).exists():
            return 0
        t = self.con.read_json(str(json_path))
        self.con.insert(table_name, t, database=schema)
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
