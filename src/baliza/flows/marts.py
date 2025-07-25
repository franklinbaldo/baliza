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
    """Create marts schema and all mart tables with proper Ibis aggregations."""
    logger = get_run_logger()
    logger.info("Creating marts layer...")

    try:
        con = connect()
        con.raw_sql("CREATE SCHEMA IF NOT EXISTS marts")
        logger.info("Created marts schema")

        api_requests = con.table("raw.api_requests")

        # Create extraction summary table with proper aggregations
        extraction_summary = (
            api_requests
            .filter(api_requests.http_status.between(200, 299))
            .group_by([
                api_requests.ingestion_date,
                api_requests.endpoint
            ])
            .aggregate(
                request_count=ibis._.count(),
                total_bytes=ibis._.payload_size.sum(),
                total_mb=(ibis._.payload_size.sum() / (1024 * 1024)).round(2),
                first_extraction=ibis._.collected_at.min(),
                last_extraction=ibis._.collected_at.max(),
                unique_payloads=ibis._.payload_sha256.nunique(),
                avg_payload_size=ibis._.payload_size.mean().round(0),
            )
            .mutate(
                duplicate_requests=ibis._.request_count - ibis._.unique_payloads,
                deduplication_ratio=(
                    (ibis._.request_count - ibis._.unique_payloads) * 100.0 / ibis._.request_count
                ).round(2)
            )
            .order_by([ibis.desc(ibis._.ingestion_date), ibis._.endpoint])
        )
        con.create_table("marts.extraction_summary", extraction_summary, overwrite=True)
        logger.info("Created marts.extraction_summary table")

        # Create data quality table with daily metrics
        data_quality = (
            api_requests
            .group_by([
                ibis._.collected_at.truncate("D").name("date"),
                ibis._.endpoint
            ])
            .aggregate(
                total_requests=ibis._.count(),
                successful_requests=ibis._.http_status.between(200, 299).sum(),
                failed_requests=(~ibis._.http_status.between(200, 299)).sum(),
                avg_payload_size=ibis._.payload_size.mean().round(0),
                min_payload_size=ibis._.payload_size.min(),
                max_payload_size=ibis._.payload_size.max(),
                unique_payloads=ibis._.payload_sha256.nunique(),
                total_bytes=ibis._.payload_size.sum(),
            )
            .mutate(
                success_rate_pct=(
                    ibis._.successful_requests * 100.0 / ibis._.total_requests
                ).round(2),
                duplicate_payloads=ibis._.total_requests - ibis._.unique_payloads,
                total_mb=(ibis._.total_bytes / (1024 * 1024)).round(2)
            )
            .order_by([ibis.desc(ibis._.date), ibis._.endpoint])
        )
        con.create_table("marts.data_quality", data_quality, overwrite=True)
        logger.info("Created marts.data_quality table")

        # Create endpoint performance metrics
        endpoint_performance = (
            api_requests
            .filter(api_requests.http_status.between(200, 299))
            .group_by(api_requests.endpoint)
            .aggregate(
                total_requests=ibis._.count(),
                unique_dates=ibis._.ingestion_date.nunique(),
                first_seen=ibis._.collected_at.min(),
                last_seen=ibis._.collected_at.max(),
                total_records=ibis._.count(),  # This would be better with actual record counts from staging
                total_mb=(ibis._.payload_size.sum() / (1024 * 1024)).round(2),
                avg_payload_size=ibis._.payload_size.mean().round(0),
                unique_payloads=ibis._.payload_sha256.nunique(),
            )
            .mutate(
                requests_per_day=(
                    ibis._.total_requests.cast("float64") / ibis._.unique_dates
                ).round(1),
                deduplication_efficiency=(
                    (ibis._.total_requests - ibis._.unique_payloads) * 100.0 / ibis._.total_requests
                ).round(2)
            )
            .order_by([ibis.desc(ibis._.total_requests)])
        )
        con.create_table("marts.endpoint_performance", endpoint_performance, overwrite=True)
        logger.info("Created marts.endpoint_performance table")

        # Get mart table counts for verification
        summary_count = con.table("marts.extraction_summary").count().execute()
        quality_count = con.table("marts.data_quality").count().execute()
        performance_count = con.table("marts.endpoint_performance").count().execute()

        return {
            "extraction_summary_count": summary_count,
            "data_quality_count": quality_count,
            "endpoint_performance_count": performance_count,
        }

    except Exception as e:
        logger.error(f"Failed to create marts layer: {e}")
        raise


@flow(name="marts_creation", log_prints=True)
def marts_creation() -> Dict[str, Any]:
    """
    Create marts (analytics) layer tables with proper Ibis aggregations
    """
    logger = get_run_logger()
    start_time = datetime.now()

    logger.info("Starting marts creation...")

    try:
        # Create marts layer
        counts = create_marts_layer()

        duration = (datetime.now() - start_time).total_seconds()
        total_records = sum(counts.values())

        result = {
            "status": "success",
            "duration_seconds": duration,
            **counts,
            "total_mart_records": total_records,
            "marts_created": list(counts.keys()),
            "using_ibis": True,
        }

        logger.info(
            f"Marts creation completed: "
            f"{total_records} analytical records across {len(counts)} mart tables in {duration:.2f}s"
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


@task(name="export_marts_to_parquet", retries=1)
def export_marts_to_parquet(output_dir: str = "data/parquet") -> Dict[str, str]:
    """Export marts tables to Parquet files."""
    logger = get_run_logger()
    
    try:
        import os
        con = connect()
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # List of mart tables to export
        mart_tables = [
            "marts.extraction_summary",
            "marts.data_quality", 
            "marts.endpoint_performance"
        ]
        
        exported_files = {}
        
        for table_name in mart_tables:
            try:
                table = con.table(table_name)
                table_df = table.execute()
                
                # Generate filename
                clean_name = table_name.replace("marts.", "")
                filename = f"{clean_name}.parquet"
                filepath = os.path.join(output_dir, filename)
                
                # Export to Parquet using Polars if available, otherwise pandas
                try:
                    import polars as pl
                    pl_df = pl.from_pandas(table_df)
                    pl_df.write_parquet(filepath)
                    logger.info(f"Exported {table_name} to {filepath} using Polars")
                except ImportError:
                    table_df.to_parquet(filepath, index=False)
                    logger.info(f"Exported {table_name} to {filepath} using pandas")
                
                exported_files[clean_name] = filepath
                
            except Exception as e:
                logger.warning(f"Failed to export {table_name}: {e}")
                continue
        
        return exported_files
        
    except Exception as e:
        logger.error(f"Failed to export marts to Parquet: {e}")
        raise
