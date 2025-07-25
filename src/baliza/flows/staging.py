"""
Staging data transformation flows using Prefect and Ibis
"""

from datetime import datetime
from typing import Dict, Any

from prefect import flow, task, get_run_logger

from ..backend import connect


@task(name="create_staging_view", retries=1)
def create_staging_view(endpoint_name: str, view_name: str) -> bool:
    """Create a staging view for a specific endpoint."""
    logger = get_run_logger()

    try:
        con = connect()
        con.raw_sql("CREATE SCHEMA IF NOT EXISTS staging")
        api_requests = con.table("raw.api_requests")
        expr = api_requests.filter(api_requests.endpoint == endpoint_name).filter(
            api_requests.http_status == 200
        )
        con.create_view(f"staging.{view_name}", expr, overwrite=True)
        logger.info(f"Created staging.{view_name} view")
        return True
    except Exception as e:
        logger.error(f"Failed to create staging view for {endpoint_name}: {e}")
        raise


@task(name="create_staging_views", retries=1)
def create_staging_views() -> bool:
    """Create staging views for data transformation"""
    logger = get_run_logger()
    logger.info("Creating staging views...")
    # This task now orchestrates the creation of individual views.
    # We can run these in parallel.
    endpoints = {
        "contratacoes_publicacao": "contratacoes",
        "contratos": "contratos",
        "atas": "atas",
    }
    tasks = []
    for endpoint, view in endpoints.items():
        task = create_staging_view.submit(endpoint, view)
        tasks.append(task)

    # Wait for all tasks to complete
    for task in tasks:
        task.result()

    logger.info("All staging views created.")
    return True


@flow(name="staging_transformation", log_prints=True)
def staging_transformation() -> Dict[str, Any]:
    """
    Transform raw data into staging layer
    """
    logger = get_run_logger()
    start_time = datetime.now()

    logger.info("Starting staging transformation...")

    try:
        # Create staging views
        create_staging_views()

        # Get row counts for verification
        con = connect()
        contratacoes_count = con.table("staging.contratacoes").count().execute()
        contratos_count = con.table("staging.contratos").count().execute()
        atas_count = con.table("staging.atas").count().execute()

        duration = (datetime.now() - start_time).total_seconds()

        result = {
            "status": "success",
            "duration_seconds": duration,
            "contratacoes_count": contratacoes_count,
            "contratos_count": contratos_count,
            "atas_count": atas_count,
            "total_staging_records": contratacoes_count + contratos_count + atas_count,
        }

        logger.info(
            f"Staging transformation completed: "
            f"{result['total_staging_records']} records in {duration:.2f}s"
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
