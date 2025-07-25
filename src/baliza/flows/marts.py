"""
Marts (analytics) layer flows using Prefect and Ibis
"""

from datetime import datetime
from typing import Dict, Any

from prefect import flow, task, get_run_logger
import ibis
from ..backend import connect


@task(name="create_marts_layer", retries=1)
def create_marts_layer() -> Dict[str, int]:
    """Create marts schema and all mart tables."""
    logger = get_run_logger()
    logger.info("Creating marts layer...")

    try:
        con = connect()
        con.create_schema("marts", overwrite=True)
        logger.info("Created marts schema")

        # Create summary table
        api_requests = con.table("raw.api_requests")
        extraction_summary = api_requests.filter(
            api_requests.http_status == 200
        ).summary(
            [
                "ingestion_date",
                "endpoint",
                "request_count",
                "total_bytes",
                "total_mb",
                "first_extraction",
                "last_extraction",
                "unique_payloads",
            ]
        )
        con.create_table("marts.extraction_summary", extraction_summary, overwrite=True)
        logger.info("Created marts.extraction_summary table")

        # Create data quality table
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

        # Get mart table counts for verification
        summary_count = con.table("marts.extraction_summary").count().execute()
        quality_count = con.table("marts.data_quality").count().execute()

        return {
            "extraction_summary_count": summary_count,
            "data_quality_count": quality_count,
        }

    except Exception as e:
        logger.error(f"Failed to create marts layer: {e}")
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
        # Create marts layer
        counts = create_marts_layer()

        duration = (datetime.now() - start_time).total_seconds()

        result = {
            "status": "success",
            "duration_seconds": duration,
            **counts,
            "total_mart_records": sum(counts.values()),
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
