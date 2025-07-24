import ibis
from ibis.backends.duckdb.Backend import Backend
from .config import settings

def connect() -> Backend:
    """Connect to the DuckDB database and set pragmas."""
    con = ibis.duckdb.connect(settings.DATABASE_PATH)
    con.raw_sql("PRAGMA threads=8;")
    return con
