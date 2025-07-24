"""
Staging data transformation flows using Prefect and Ibis
"""

from datetime import datetime
from typing import Dict, Any

from prefect import flow, task, get_run_logger

from ..backend import connect, load_sql_file


@task(name="create_staging_views", retries=1)
def create_staging_views() -> bool:
    """Create staging views for data transformation"""
    logger = get_run_logger()

    try:
        con = connect()

        # Create staging schema if not exists
        # TODO: Consider using Ibis schema management instead of raw SQL
        con.raw_sql("CREATE SCHEMA IF NOT EXISTS staging")

        # Create staging views using external SQL files
        # TODO: Replace SQL files with Ibis view definitions for better maintainability
        # FIXME: Should use con.create_view() with Ibis expressions instead of raw SQL
        con.raw_sql(load_sql_file("staging_contratacoes.sql"))
        logger.info("Created staging.contratacoes view")

        # TODO: Convert to Ibis: con.create_view("staging.contratos", ibis_expr)
        con.raw_sql(load_sql_file("staging_contratos.sql"))
        logger.info("Created staging.contratos view")

        # TODO: Convert to Ibis: con.create_view("staging.atas", ibis_expr)
        con.raw_sql(load_sql_file("staging_atas.sql"))
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
        # TODO: Replace raw SQL with Ibis expressions for better type safety and composability
        # FIXME: Should use con.table("staging.contratacoes").count().execute() instead
        contratacoes_count = con.raw_sql(
            "SELECT COUNT(*) as cnt FROM staging.contratacoes"
        ).fetchone()[0]
        # TODO: Use Ibis table expressions: con.table("staging.contratos").count().execute()
        contratos_count = con.raw_sql(
            "SELECT COUNT(*) as cnt FROM staging.contratos"
        ).fetchone()[0]
        # TODO: Use Ibis table expressions: con.table("staging.atas").count().execute()
        atas_count = con.raw_sql("SELECT COUNT(*) as cnt FROM staging.atas").fetchone()[
            0
        ]

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
