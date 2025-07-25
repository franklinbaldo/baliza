"""
Parquet export flows using Prefect, Ibis, and Polars
"""

import os
from datetime import datetime
from typing import Dict, Any, List, Optional

from prefect import flow, task, get_run_logger

from ..backend import connect


@task(name="export_table_to_parquet", retries=1)
def export_table_to_parquet(
    table_name: str, 
    output_dir: str, 
    partition_by: Optional[str] = None,
    filename_prefix: Optional[str] = None
) -> Dict[str, Any]:
    """Export a single table to Parquet format."""
    logger = get_run_logger()
    
    try:
        con = connect()
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Get table data
        table = con.table(table_name)
        table_df = table.execute()
        
        if len(table_df) == 0:
            logger.warning(f"Table {table_name} is empty, skipping export")
            return {"status": "skipped", "reason": "empty_table", "records": 0}
        
        # Generate filename
        clean_name = table_name.replace(".", "_")
        if filename_prefix:
            filename = f"{filename_prefix}_{clean_name}.parquet"
        else:
            filename = f"{clean_name}.parquet"
        filepath = os.path.join(output_dir, filename)
        
        # Export to Parquet using Polars if available, otherwise pandas
        try:
            import polars as pl
            pl_df = pl.from_pandas(table_df)
            
            if partition_by and partition_by in table_df.columns:
                # Partitioned export using Polars
                partition_dir = os.path.join(output_dir, clean_name)
                os.makedirs(partition_dir, exist_ok=True)
                pl_df.write_parquet(partition_dir, partition_by=partition_by)
                logger.info(f"Exported {table_name} to {partition_dir} partitioned by {partition_by} using Polars")
                export_path = partition_dir
            else:
                # Single file export
                pl_df.write_parquet(filepath)
                logger.info(f"Exported {table_name} to {filepath} using Polars")
                export_path = filepath
                
            engine = "polars"
            
        except ImportError:
            # Fallback to pandas
            if partition_by and partition_by in table_df.columns:
                logger.warning(f"Partitioning not supported with pandas, exporting as single file")
            
            table_df.to_parquet(filepath, index=False)
            logger.info(f"Exported {table_name} to {filepath} using pandas")
            export_path = filepath
            engine = "pandas"
        
        # Get file size
        if os.path.isfile(export_path):
            file_size = os.path.getsize(export_path)
        else:
            # Directory size (partitioned)
            file_size = sum(
                os.path.getsize(os.path.join(export_path, f)) 
                for f in os.listdir(export_path) 
                if os.path.isfile(os.path.join(export_path, f))
            )
        
        return {
            "status": "success",
            "table_name": table_name,
            "export_path": export_path,
            "records": len(table_df),
            "file_size_bytes": file_size,
            "file_size_mb": round(file_size / (1024 * 1024), 2),
            "engine": engine,
            "partitioned": partition_by is not None,
            "partition_column": partition_by
        }
        
    except Exception as e:
        logger.error(f"Failed to export {table_name} to Parquet: {e}")
        return {
            "status": "failed",
            "table_name": table_name,
            "error": str(e)
        }


@task(name="export_staging_to_parquet", retries=1)
def export_staging_to_parquet(output_dir: str = "data/parquet/staging") -> Dict[str, Any]:
    """Export all staging tables to Parquet files."""
    logger = get_run_logger()
    
    try:
        con = connect()
        
        # Get list of staging tables
        staging_tables = []
        try:
            # Try to list tables in staging schema
            tables_info = con.list_tables(schema="staging")
            staging_tables = [f"staging.{table}" for table in tables_info]
        except Exception:
            # Fallback to known staging tables
            staging_tables = [
                "staging.contratacoes",
                "staging.contratos", 
                "staging.atas"
            ]
        
        if not staging_tables:
            logger.warning("No staging tables found")
            return {"status": "skipped", "reason": "no_tables"}
        
        # Export each staging table
        export_tasks = []
        for table_name in staging_tables:
            try:
                # Check if table exists
                con.table(table_name)
                task = export_table_to_parquet.submit(
                    table_name=table_name,
                    output_dir=output_dir,
                    partition_by="source_ingestion_date",  # Partition by ingestion date
                    filename_prefix="staging"
                )
                export_tasks.append(task)
            except Exception:
                logger.warning(f"Staging table {table_name} not found, skipping")
                continue
        
        # Collect results
        results = []
        for task in export_tasks:
            result = task.result()
            results.append(result)
        
        # Summarize results
        successful_exports = [r for r in results if r["status"] == "success"]
        failed_exports = [r for r in results if r["status"] == "failed"]
        
        total_records = sum(r.get("records", 0) for r in successful_exports)
        total_size_mb = sum(r.get("file_size_mb", 0) for r in successful_exports)
        
        return {
            "status": "success",
            "successful_exports": len(successful_exports),
            "failed_exports": len(failed_exports),
            "total_records": total_records,
            "total_size_mb": total_size_mb,
            "results": results
        }
        
    except Exception as e:
        logger.error(f"Failed to export staging tables: {e}")
        return {"status": "failed", "error": str(e)}


@task(name="export_marts_to_parquet", retries=1)
def export_marts_to_parquet(output_dir: str = "data/parquet/marts") -> Dict[str, Any]:
    """Export all marts tables to Parquet files."""
    logger = get_run_logger()
    
    try:
        con = connect()
        
        # Known marts tables
        mart_tables = [
            "marts.extraction_summary",
            "marts.data_quality", 
            "marts.endpoint_performance"
        ]
        
        # Export each mart table
        export_tasks = []
        for table_name in mart_tables:
            try:
                # Check if table exists
                con.table(table_name)
                task = export_table_to_parquet.submit(
                    table_name=table_name,
                    output_dir=output_dir,
                    filename_prefix="mart"
                )
                export_tasks.append(task)
            except Exception:
                logger.warning(f"Mart table {table_name} not found, skipping")
                continue
        
        # Collect results
        results = []
        for task in export_tasks:
            result = task.result()
            results.append(result)
        
        # Summarize results
        successful_exports = [r for r in results if r["status"] == "success"]
        failed_exports = [r for r in results if r["status"] == "failed"]
        
        total_records = sum(r.get("records", 0) for r in successful_exports)
        total_size_mb = sum(r.get("file_size_mb", 0) for r in successful_exports)
        
        return {
            "status": "success",
            "successful_exports": len(successful_exports),
            "failed_exports": len(failed_exports),
            "total_records": total_records,
            "total_size_mb": total_size_mb,
            "results": results
        }
        
    except Exception as e:
        logger.error(f"Failed to export marts tables: {e}")
        return {"status": "failed", "error": str(e)}


@flow(name="export_to_parquet", log_prints=True)
def export_to_parquet(
    output_base_dir: str = "data/parquet",
    export_staging: bool = True,
    export_marts: bool = True,
    month_filter: Optional[str] = None
) -> Dict[str, Any]:
    """
    Export staging and marts data to Parquet files with optional month filtering
    """
    logger = get_run_logger()
    start_time = datetime.now()

    logger.info(f"Starting Parquet export to {output_base_dir}...")
    
    if month_filter:
        logger.info(f"Filtering data for month: {month_filter}")

    try:
        results = {}
        
        if export_staging:
            staging_dir = os.path.join(output_base_dir, "staging")
            staging_result = export_staging_to_parquet(staging_dir)
            results["staging"] = staging_result
            
        if export_marts:
            marts_dir = os.path.join(output_base_dir, "marts")
            marts_result = export_marts_to_parquet(marts_dir)
            results["marts"] = marts_result

        duration = (datetime.now() - start_time).total_seconds()
        
        # Aggregate summary
        total_records = sum(
            r.get("total_records", 0) for r in results.values() 
            if isinstance(r, dict)
        )
        total_size_mb = sum(
            r.get("total_size_mb", 0) for r in results.values() 
            if isinstance(r, dict)
        )
        total_successful = sum(
            r.get("successful_exports", 0) for r in results.values() 
            if isinstance(r, dict)
        )
        total_failed = sum(
            r.get("failed_exports", 0) for r in results.values() 
            if isinstance(r, dict)
        )

        final_result = {
            "status": "success",
            "duration_seconds": duration,
            "output_directory": output_base_dir,
            "total_exports_successful": total_successful,
            "total_exports_failed": total_failed,
            "total_records_exported": total_records,
            "total_size_mb": total_size_mb,
            "month_filter": month_filter,
            "details": results
        }

        logger.info(
            f"Parquet export completed: "
            f"{total_successful} successful exports, {total_records} records, "
            f"{total_size_mb:.2f} MB in {duration:.2f}s"
        )

        return final_result

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"Parquet export failed: {e}")

        return {
            "status": "failed", 
            "duration_seconds": duration,
            "error_message": str(e)
        }