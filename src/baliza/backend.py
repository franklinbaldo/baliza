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
    # FIXME: This is a hacky way to get a logger outside of a flow context.
    # It would be better to have a more robust logging setup.
    logger = logging.getLogger(__name__)


from .utils.io import load_sql_file


def execute_sql_file(
    con: Backend, filename: str, params: Dict[str, Any] = None
) -> None:
    """Execute SQL file with optional parameter substitution"""
    sql_content = load_sql_file(filename)

    # TODO: This is a simple implementation. For more complex scenarios,
    # consider using a more robust templating engine like Jinja2.
    if params:
        for key, value in params.items():
            sql_content = sql_content.replace(f"${{{key}}}", str(value))

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
    con.raw_sql(f"PRAGMA enable_progress_bar={'true' if settings.duckdb_enable_progress_bar else 'false'};")

    logger.info(f"Connected to DuckDB at {settings.database_path}")
    return con


def init_database_schema(con: Backend = None) -> None:
    """Initialize database schema using external SQL file"""

    if con is None:
        con = connect()

    logger.info("Initializing database schema...")
    execute_sql_file(con, "init_schema.sql")
    logger.info("Database schema initialized successfully")


def get_table_stats(con: Backend = None) -> List[Dict[str, Any]]:
    """Get statistics for all tables using Ibis expressions"""

    if con is None:
        con = connect()

    try:
        # TODO: This is not very scalable. If new tables are added, they will
        # not be included in the stats. It would be better to dynamically
        # get the list of tables and generate the stats for each one.
        api_requests = con.table("api_requests", schema="raw")
        execution_log = con.table("execution_log", schema="meta")
        failed_requests = con.table("failed_requests", schema="meta")

        api_requests_stats = api_requests.aggregate(
            record_count=api_requests.count(),
            oldest_record=api_requests.collected_at.min(),
            newest_record=api_requests.collected_at.max(),
            unique_endpoints=api_requests.endpoint.nunique(),
            unique_dates=api_requests.ingestion_date.nunique(),
            avg_payload_size=api_requests.payload_size.mean(),
            total_payload_size=api_requests.payload_size.sum(),
        ).mutate(table_name=ibis.literal("raw.api_requests"))

        execution_log_stats = execution_log.aggregate(
            record_count=execution_log.count(),
            oldest_record=execution_log.start_time.min(),
            newest_record=execution_log.start_time.max(),
            unique_endpoints=execution_log.flow_name.nunique(),
            unique_dates=execution_log.start_time.date().nunique(),
            avg_payload_size=execution_log.records_processed.mean(),
            total_payload_size=execution_log.records_processed.sum(),
        ).mutate(table_name=ibis.literal("meta.execution_log"))

        failed_requests_stats = failed_requests.aggregate(
            record_count=failed_requests.count(),
            oldest_record=failed_requests.failed_at.min(),
            newest_record=failed_requests.failed_at.max(),
            unique_endpoints=failed_requests.original_endpoint.nunique(),
            unique_dates=failed_requests.failed_at.date().nunique(),
            avg_payload_size=failed_requests.retry_count.mean(),
            total_payload_size=ibis.case()
            .when(failed_requests.resolved, 1)
            .else_(0)
            .end()
            .sum(),
        ).mutate(table_name=ibis.literal("meta.failed_requests"))

        unioned = ibis.union(
            api_requests_stats, execution_log_stats, failed_requests_stats
        )
        result = unioned.execute().to_dict("records")
        return result
    except Exception as e:
        logger.error(f"Error getting table stats: {e}")
        return []


def cleanup_old_data(
    con: Backend = None, days_to_keep: int = 90, failed_days_to_keep: int = 30
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


def vacuum_database(con: Backend = None) -> None:
    """Optimize database by running VACUUM"""

    if con is None:
        con = connect()

    logger.info("Running database optimization...")
    con.raw_sql("VACUUM;")
    con.raw_sql("ANALYZE;")
    logger.info("Database optimization completed")
