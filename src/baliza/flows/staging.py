"""
Staging data transformation flows using Prefect and Ibis
"""

from datetime import datetime
from typing import Dict, Any

from prefect import flow, task, get_run_logger

from ..backend import connect


@task(name="create_staging_views", retries=1)
def create_staging_views() -> bool:
    """Create staging views for data transformation"""
    logger = get_run_logger()

    try:
        con = connect()

        # TODO: This task creates views for all endpoints in a single task.
        # It would be better to break this down into smaller tasks, one for
        # each view, to improve parallelism and make the flow more modular.
        # Create staging schema if not exists
        con.raw_sql("CREATE SCHEMA IF NOT EXISTS staging")

        # Define Ibis expressions for views
        api_requests = con.table("raw.api_requests")

        # contratacoes
        contratacoes_expr = api_requests.filter(
            api_requests.endpoint == "contratacoes_publicacao"
        ).filter(api_requests.http_status == 200)
        con.create_view("staging.contratacoes", contratacoes_expr, overwrite=True)
        logger.info("Created staging.contratacoes view")

        # contratos
        contratos_expr = api_requests.filter(
            api_requests.endpoint == "contratos"
        ).filter(api_requests.http_status == 200)
        con.create_view("staging.contratos", contratos_expr, overwrite=True)
        logger.info("Created staging.contratos view")

        # atas
        atas_expr = api_requests.filter(api_requests.endpoint == "atas").filter(
            api_requests.http_status == 200
        )
        con.create_view("staging.atas", atas_expr, overwrite=True)
        logger.info("Created staging.atas view")

        return True

    except Exception as e:
        logger.error(f"Failed to create staging views: {e}")
        raise


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
