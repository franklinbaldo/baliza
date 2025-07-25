"""
Staging data transformation flows using Prefect and Ibis
"""

import json
import zlib
from datetime import datetime
from typing import Dict, Any, List

from prefect import flow, task, get_run_logger
import ibis

from ..backend import connect


@task(name="extract_and_normalize_payloads", retries=1)
def extract_and_normalize_payloads(endpoint_pattern: str, table_name: str) -> int:
    """Extract and normalize JSON payloads for a specific endpoint pattern."""
    logger = get_run_logger()

    try:
        con = connect()
        
        # Get successful requests with payloads for this endpoint pattern
        api_requests = con.table("raw.api_requests")
        hot_payloads = con.table("raw.hot_payloads")
        
        # Join to get compressed payloads
        query = (
            api_requests
            .join(hot_payloads, api_requests.payload_sha256 == hot_payloads.payload_sha256)
            .filter(
                (api_requests.http_status >= 200) & 
                (api_requests.http_status <= 299) &
                api_requests.endpoint.like(f"%{endpoint_pattern}%")
            )
            .select([
                api_requests.request_id,
                api_requests.ingestion_date,
                api_requests.endpoint,
                api_requests.collected_at,
                hot_payloads.payload_compressed
            ])
        )
        
        # Execute query to get data
        raw_data = query.execute()
        
        if len(raw_data) == 0:
            logger.warning(f"No data found for endpoint pattern: {endpoint_pattern}")
            return 0
        
        # Extract and normalize JSON data
        normalized_records = []
        
        for _, row in raw_data.iterrows():
            try:
                # Decompress and parse JSON
                payload_json = json.loads(
                    zlib.decompress(row['payload_compressed']).decode("utf-8")
                )
                
                # Extract records from PNCP response structure
                if 'data' in payload_json and isinstance(payload_json['data'], list):
                    for record in payload_json['data']:
                        # Add metadata to each record
                        normalized_record = {
                            'source_request_id': str(row['request_id']),
                            'source_ingestion_date': row['ingestion_date'],
                            'source_endpoint': row['endpoint'],
                            'source_collected_at': row['collected_at'],
                            **record  # Spread the actual PNCP record data
                        }
                        normalized_records.append(normalized_record)
                        
            except Exception as e:
                logger.warning(f"Failed to parse payload for request {row['request_id']}: {e}")
                continue
        
        if not normalized_records:
            logger.warning(f"No valid records extracted for {endpoint_pattern}")
            return 0
        
        # Create staging table using Ibis with normalized data
        # Note: We'll use Polars to handle the complex nested JSON data
        try:
            import polars as pl
            
            # Create Polars DataFrame from normalized records
            df = pl.DataFrame(normalized_records)
            
            # Convert to Ibis table (DuckDB can read from Polars)
            staging_table = con.read_polars(df)
            con.create_table(f"staging.{table_name}", staging_table, overwrite=True)
            
            logger.info(f"Created staging.{table_name} with {len(normalized_records)} records using Polars")
            
        except ImportError:
            # Fallback to basic approach if Polars not available
            logger.warning("Polars not available, using basic staging approach")
            
            # Create a simple staging view without full normalization
            con.create_schema("staging", if_not_exists=True)
            staging_expr = query.select([
                query.request_id.name('source_request_id'),
                query.ingestion_date.name('source_ingestion_date'), 
                query.endpoint.name('source_endpoint'),
                query.collected_at.name('source_collected_at')
            ])
            con.create_view(f"staging.{table_name}_raw", staging_expr, overwrite=True)
            logger.info(f"Created basic staging view staging.{table_name}_raw")
            
            return len(raw_data)
        
        return len(normalized_records)
        
    except Exception as e:
        logger.error(f"Failed to extract and normalize {endpoint_pattern}: {e}")
        raise


@task(name="create_staging_tables", retries=1)
def create_staging_tables() -> Dict[str, int]:
    """Create staging tables for data transformation"""
    logger = get_run_logger()
    logger.info("Creating staging tables with normalized JSON data...")
    
    # Ensure staging schema exists
    con = connect()
    con.raw_sql("CREATE SCHEMA IF NOT EXISTS staging")
    
    # Define endpoint patterns and their corresponding table names
    endpoints = {
        "contratacoes": "contratacoes",
        "contratos": "contratos", 
        "atas": "atas",
    }
    
    # Create staging tables in parallel
    tasks = []
    for endpoint_pattern, table_name in endpoints.items():
        task = extract_and_normalize_payloads.submit(endpoint_pattern, table_name)
        tasks.append((table_name, task))

    # Wait for all tasks to complete and collect results
    results = {}
    for table_name, task in tasks:
        record_count = task.result()
        results[f"{table_name}_count"] = record_count
        logger.info(f"Staging table {table_name}: {record_count} records")

    logger.info("All staging tables created.")
    return results


@flow(name="staging_transformation", log_prints=True)
def staging_transformation() -> Dict[str, Any]:
    """
    Transform raw data into staging layer with normalized JSON payloads
    """
    logger = get_run_logger()
    start_time = datetime.now()

    logger.info("Starting staging transformation...")

    try:
        # Create staging tables with normalized data
        counts = create_staging_tables()

        # Get final row counts for verification
        con = connect()
        try:
            contratacoes_count = con.table("staging.contratacoes").count().execute()
        except Exception:
            contratacoes_count = counts.get("contratacoes_count", 0)
            
        try:
            contratos_count = con.table("staging.contratos").count().execute() 
        except Exception:
            contratos_count = counts.get("contratos_count", 0)
            
        try:
            atas_count = con.table("staging.atas").count().execute()
        except Exception:
            atas_count = counts.get("atas_count", 0)

        duration = (datetime.now() - start_time).total_seconds()
        total_records = contratacoes_count + contratos_count + atas_count

        result = {
            "status": "success",
            "duration_seconds": duration,
            "contratacoes_count": contratacoes_count,
            "contratos_count": contratos_count,
            "atas_count": atas_count,
            "total_staging_records": total_records,
            "normalized_data": True,
            "using_polars": "polars" in str(counts),
        }

        logger.info(
            f"Staging transformation completed: "
            f"{total_records} normalized records in {duration:.2f}s"
        )

        return result

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"Staging transformation failed: {e}")

        return {
            "status": "failed",
            "duration_seconds": duration,
            "error_message": str(e),
        }
