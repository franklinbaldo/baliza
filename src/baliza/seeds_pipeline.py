import dlt
from pathlib import Path
import pandas as pd

@dlt.source
def seeds_source():
    seeds_dir = Path(__file__).parent / "seeds" / "domain_tables"
    for f in seeds_dir.glob("*.csv"):
        table_name = f.stem
        yield dlt.resource(pd.read_csv(f), name=table_name)

def run_seeds_pipeline():
    """
    Runs the dlt pipeline to load the seed data into DuckDB.
    """
    pipeline = dlt.pipeline(
        pipeline_name="seeds",
        destination="duckdb",
        dataset_name="seeds",
    )
    load_info = pipeline.run(seeds_source())
    print(load_info)

if __name__ == "__main__":
    run_seeds_pipeline()
