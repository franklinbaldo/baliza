from kedro.framework.project import find_pipelines
from kedro.framework.startup import bootstrap_project
from kedro.pipeline import Pipeline
from baliza_kedro.pipelines import (
    bronze_pipeline,
    silver_pipeline,
    gold_pipeline,
    master_pipeline,
)

def register_pipelines():
    return {
        "bronze": bronze_pipeline.create_pipeline(),
        "silver": silver_pipeline.create_pipeline(),
        "gold": gold_pipeline.create_pipeline(),
        "__default__": master_pipeline.create_pipeline(),
    }
