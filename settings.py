from kedro.pipeline import Pipeline
from kedro.pipeline import Pipeline
from pipelines import (
    bronze_pipeline,
    silver_pipeline,
    gold_pipeline,
    seeds_pipeline,
    master_pipeline,
)

def register_pipelines():
    return {
        "bronze": bronze_pipeline.create_pipeline(),
        "silver": silver_pipeline.create_pipeline(),
        "gold": gold_pipeline.create_pipeline(),
        "seeds": seeds_pipeline.create_pipeline(),
        "__default__": master_pipeline.create_pipeline(),
    }
