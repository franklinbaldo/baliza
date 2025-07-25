"""
Marts (analytics) layer flows using Prefect and Ibis
"""

from datetime import datetime
from typing import Dict, Any

from prefect import flow, task, get_run_logger
import ibis
from ..backend import connect


@task(name="create_marts_schema", retries=1)
def create_marts_schema() -> bool:
    """Create marts schema and tables"""
    logger = get_run_logger()

    try:
        con = connect()

        # TODO: This task is very simple and could be combined with the
        # `create_summary_table` and `create_data_quality_table` tasks to
        # reduce the number of tasks in the flow.
        # Create marts schema if not exists
        # TODO: Consider using Ibis schema management for consistency
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
        api_requests = con.table("raw.api_requests")

        # FIXME: The logic for calculating the summary is complex and could
        # be simplified by using more of the features of the Ibis library.
        extraction_summary = (
            api_requests.filter(api_requests.http_status == 200)
            .group_by(["ingestion_date", "endpoint"])
            .aggregate(
                request_count=ibis._.count(),
                total_bytes=ibis._.payload_size.sum(),
                total_mb=(ibis._.payload_size.sum() / 1024 / 1024).round(2),
                first_extraction=ibis._.collected_at.min(),
                last_extraction=ibis._.collected_at.max(),
                unique_payloads=ibis._.payload_sha256.nunique(),
            )
            .order_by([ibis.desc("ingestion_date"), "endpoint"])
        )

        con.create_table("marts.extraction_summary", extraction_summary, overwrite=True)
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
        api_requests = con.table("raw.api_requests")

        data_quality = (
            api_requests.group_by(
                date=api_requests.collected_at.truncate("D"), endpoint="endpoint"
            )
            .aggregate(
                total_requests=ibis._.count(),
                successful_requests=(
                    ibis.case().when(api_requests.http_status == 200, 1).else_(0)
                ).sum(),
                failed_requests=(
                    ibis.case().when(api_requests.http_status != 200, 1).else_(0)
                ).sum(),
                avg_payload_size=ibis._.payload_size.mean(),
                min_payload_size=ibis._.payload_size.min(),
                max_payload_size=ibis._.payload_size.max(),
                unique_payloads=ibis._.payload_sha256.nunique(),
            )
            .mutate(
                success_rate_pct=(
                    ibis._.successful_requests * 100.0 / ibis._.total_requests
                ).round(2),
                duplicate_payloads=ibis._.total_requests - ibis._.unique_payloads,
            )
            .order_by([ibis.desc("date"), "endpoint"])
        )

        con.create_table("marts.data_quality", data_quality, overwrite=True)
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
        summary_count = con.table("marts.extraction_summary").count().execute()
        quality_count = con.table("marts.data_quality").count().execute()

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
