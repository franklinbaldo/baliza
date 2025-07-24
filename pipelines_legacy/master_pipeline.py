from kedro.pipeline import Pipeline
from .bronze_pipeline import create_pipeline as create_bronze_pipeline
from .seeds_pipeline import create_pipeline as create_seeds_pipeline

def create_pipeline(**kwargs):
    seeds_pipeline = create_seeds_pipeline()
    bronze_pipeline = create_bronze_pipeline()

    return seeds_pipeline + bronze_pipeline
