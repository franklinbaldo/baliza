import ibis
from ibis.backends.duckdb import Backend
from prefect import get_run_logger
from pathlib import Path
from typing import Dict, Any, List
import logging
from .config import settings

try:
    logger = get_run_logger()
except Exception:  # pragma: no cover - outside flow context
    logger = logging.getLogger(__name__)


def load_sql_file(filename: str) -> str:
    """Load SQL file from src/baliza/sql/ directory"""
    sql_path = Path(__file__).parent / "sql" / filename
    try:
        with open(sql_path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        raise FileNotFoundError(f"SQL file not found: {sql_path}")


def execute_sql_file(
    con: Backend, filename: str, params: Dict[str, Any] = None
) -> None:
    """Execute SQL file with optional parameter substitution"""
    sql_content = load_sql_file(filename)

    # Simple parameter substitution for ${param} style
    if params:
        for key, value in params.items():
            sql_content = sql_content.replace(f"${{{key}}}", str(value))

    con.raw_sql(sql_content)


def connect() -> Backend:
    """Connect to the DuckDB database and set pragmas."""

    # Ensure data directory exists
    db_path = Path(settings.DATABASE_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = ibis.duckdb.connect(settings.DATABASE_PATH)

    # Optimize DuckDB settings
    con.raw_sql("PRAGMA threads=8;")
    con.raw_sql("PRAGMA memory_limit='4GB';")
    # TODO: Make temp_directory configurable instead of hardcoded path
    con.raw_sql("PRAGMA temp_directory='data/tmp';")
    con.raw_sql("PRAGMA enable_progress_bar=true;")

    logger.info(f"Connected to DuckDB at {settings.DATABASE_PATH}")
    return con


def init_database_schema(con: Backend = None) -> None:
    """Initialize database schema using external SQL file"""

    if con is None:
        con = connect()

    logger.info("Initializing database schema...")
    execute_sql_file(con, "init_schema.sql")
    logger.info("Database schema initialized successfully")


def get_table_stats(con: Backend = None) -> List[Dict[str, Any]]:
    """Get statistics for all tables using SQL file"""

    if con is None:
        con = connect()

    try:
        result = con.raw_sql(load_sql_file("get_table_stats.sql")).fetchall()
        return [dict(row) for row in result] if result else []
    except Exception as e:
        logger.error(f"Error getting table stats: {e}")
        return []


def cleanup_old_data(con: Backend = None, days_to_keep: int = 90) -> None:
    """Cleanup old data based on retention policy using SQL file"""

    if con is None:
        con = connect()

    logger.info(f"Cleaning up data older than {days_to_keep} days...")
    execute_sql_file(con, "cleanup_old_data.sql", {"days_to_keep": days_to_keep})
    logger.info("Data cleanup completed")


def vacuum_database(con: Backend = None) -> None:
    """Optimize database by running VACUUM"""

    if con is None:
        con = connect()

    logger.info("Running database optimization...")
    con.raw_sql("VACUUM;")
    con.raw_sql("ANALYZE;")
    logger.info("Database optimization completed")
