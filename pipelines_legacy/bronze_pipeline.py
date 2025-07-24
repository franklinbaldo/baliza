from kedro.pipeline import Pipeline, node
from .bronze_nodes import (
    create_bronze_content_analysis,
    create_bronze_pncp_raw,
    create_bronze_pncp_requests,
)

def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=create_bronze_content_analysis,
                inputs=["pncp_content", "pncp_requests"],
                outputs="bronze_content_analysis",
                name="create_bronze_content_analysis",
            ),
            node(
                func=create_bronze_pncp_raw,
                inputs="pncp_content",
                outputs="bronze_pncp_raw",
                name="create_bronze_pncp_raw",
            ),
            node(
                func=create_bronze_pncp_requests,
                inputs="pncp_requests",
                outputs="bronze_pncp_requests",
                name="create_bronze_pncp_requests",
            ),
        ]
    )
