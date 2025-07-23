from kedro.pipeline import Pipeline, node
from .nodes import my_node

def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=my_node,
                inputs="input_data",
                outputs="output_data",
                name="my_node",
            ),
        ]
    )
