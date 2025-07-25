import ibis
from ibis.backends.duckdb import Backend
from pathlib import Path
from typing import Dict, Any, List
from jinja2 import Template
from .config import settings
from .logger import get_logger
from .legacy.utils.io import load_sql_file

logger = get_logger(__name__)


from typing import Optional


def execute_sql_file(
    con: Backend, filename: str, params: Optional[Dict[str, Any]] = None
) -> None:
    """Execute SQL file with optional parameter substitution"""
    sql_template = load_sql_file(filename)
    template = Template(sql_template)
    sql_content = template.render(params or {})
    con.raw_sql(sql_content)


def connect() -> Backend:
    """Connect to the DuckDB database and set pragmas."""

    # Ensure data directory exists
    db_path = Path(settings.database_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = ibis.duckdb.connect(settings.database_path)

    # Ensure temp directory exists
    temp_path = Path(settings.temp_directory)
    temp_path.mkdir(parents=True, exist_ok=True)

    con.raw_sql(f"PRAGMA threads={settings.duckdb_threads};")
    con.raw_sql(f"PRAGMA memory_limit='{settings.duckdb_memory_limit}';")
    con.raw_sql(f"PRAGMA temp_directory='{settings.temp_directory}';")
    con.raw_sql(
        f"PRAGMA enable_progress_bar={'true' if settings.duckdb_enable_progress_bar else 'false'};"
    )

    logger.info(f"Connected to DuckDB at {settings.database_path}")
    return con


def init_database_schema(con: Optional[Backend] = None) -> None:
    """Initialize database schema using external SQL file"""

    if con is None:
        con = connect()

    logger.info("Initializing database schema...")
    execute_sql_file(con, "init_schema.sql")
    logger.info("Database schema initialized successfully")


def get_table_stats(con: Optional[Backend] = None) -> List[Dict[str, Any]]:
    """Get statistics for all tables using Ibis expressions"""

    if con is None:
        con = connect()

    try:
        stats = []
        for table_name in con.list_tables():
            try:
                table = con.table(table_name)
                # A more generic way to get stats, might need adjustment based on common columns
                if "collected_at" in table.columns:
                    stat = table.aggregate(
                        record_count=table.count(),
                        oldest_record=table.collected_at.min(),
                        newest_record=table.collected_at.max(),
                    ).mutate(table_name=ibis.literal(table_name))
                    stats.append(stat)
            except Exception as e:
                logger.warning(f"Could not get stats for table {table_name}: {e}")

        if not stats:
            return []

        unioned = ibis.union(*stats)
        result = unioned.execute().to_dict("records")
        return result
    except Exception as e:
        logger.error(f"Error getting table stats: {e}")
        return []


def cleanup_old_data(
    con: Optional[Backend] = None, days_to_keep: int = 90, failed_days_to_keep: int = 30
) -> None:
    """Cleanup old data based on retention policy using Ibis expressions"""

    if con is None:
        con = connect()

    logger.info(f"Cleaning up data older than {days_to_keep} days...")

    try:
        # Cleanup execution_log
        execution_log = con.table("execution_log", schema="meta")
        con.delete(
            execution_log,
            [
                execution_log.start_time
                < (ibis.now() - ibis.interval(days=days_to_keep))
            ],
        )

        # Cleanup failed_requests
        failed_requests = con.table("failed_requests", schema="meta")
        con.delete(
            failed_requests,
            [
                failed_requests.failed_at
                < (ibis.now() - ibis.interval(days=failed_days_to_keep)),
                failed_requests.resolved,
            ],
        )
        logger.info("Data cleanup completed")
    except Exception as e:
        logger.error(f"Error cleaning up old data: {e}")


def vacuum_database(con: Optional[Backend] = None) -> None:
    """Optimize database by running VACUUM"""

    if con is None:
        con = connect()

    logger.info("Running database optimization...")
    con.raw_sql("VACUUM;")
    con.raw_sql("ANALYZE;")
    logger.info("Database optimization completed")
