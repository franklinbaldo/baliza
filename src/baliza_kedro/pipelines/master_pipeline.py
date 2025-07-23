from kedro.pipeline import Pipeline
from .bronze_pipeline import create_pipeline as create_bronze_pipeline
from .silver_pipeline import create_pipeline as create_silver_pipeline
from .gold_pipeline import create_pipeline as create_gold_pipeline

def create_pipeline(**kwargs):
    bronze_pipeline = create_bronze_pipeline()
    silver_pipeline = create_silver_pipeline()
    gold_pipeline = create_gold_pipeline()

    return bronze_pipeline + silver_pipeline + gold_pipeline
