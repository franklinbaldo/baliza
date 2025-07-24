"""
Marts (analytics) layer flows using Prefect and Ibis
"""

from datetime import datetime
from typing import Dict, Any

from prefect import flow, task, get_run_logger

from ..backend import connect, load_sql_file


@task(name="create_marts_schema", retries=1)
def create_marts_schema() -> bool:
    """Create marts schema and tables"""
    logger = get_run_logger()

    try:
        con = connect()

        # Create marts schema if not exists
        con.raw_sql("CREATE SCHEMA IF NOT EXISTS marts")

        logger.info("Created marts schema")
        return True

    except Exception as e:
        logger.error(f"Failed to create marts schema: {e}")
        raise


@task(name="create_summary_table", retries=1)
def create_summary_table() -> bool:
    """Create extraction summary mart table"""
    logger = get_run_logger()

    try:
        con = connect()

        # Create summary table using external SQL file
        con.raw_sql(load_sql_file("marts_extraction_summary.sql"))
        logger.info("Created marts.extraction_summary table")

        return True

    except Exception as e:
        logger.error(f"Failed to create summary table: {e}")
        raise


@task(name="create_data_quality_table", retries=1)
def create_data_quality_table() -> bool:
    """Create data quality monitoring table"""
    logger = get_run_logger()

    try:
        con = connect()

        # Create data quality table using external SQL file
        con.raw_sql(load_sql_file("marts_data_quality.sql"))
        logger.info("Created marts.data_quality table")

        return True

    except Exception as e:
        logger.error(f"Failed to create data quality table: {e}")
        raise


@flow(name="marts_creation", log_prints=True)
def marts_creation() -> Dict[str, Any]:
    """
    Create marts (analytics) layer tables
    """
    logger = get_run_logger()
    start_time = datetime.now()

    logger.info("Starting marts creation...")

    try:
        # Create marts schema
        create_marts_schema()

        # Create mart tables
        create_summary_table()
        create_data_quality_table()

        # Get mart table counts for verification
        con = connect()
        summary_count = con.raw_sql(
            "SELECT COUNT(*) as cnt FROM marts.extraction_summary"
        ).fetchone()[0]
        quality_count = con.raw_sql(
            "SELECT COUNT(*) as cnt FROM marts.data_quality"
        ).fetchone()[0]

        duration = (datetime.now() - start_time).total_seconds()

        result = {
            "status": "success",
            "duration_seconds": duration,
            "extraction_summary_count": summary_count,
            "data_quality_count": quality_count,
            "total_mart_records": summary_count + quality_count,
        }

        logger.info(
            f"Marts creation completed: "
            f"{result['total_mart_records']} records in {duration:.2f}s"
        )

        return result

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"Marts creation failed: {e}")

        return {
            "status": "failed",
            "duration_seconds": duration,
            "error_message": str(e),
        }
