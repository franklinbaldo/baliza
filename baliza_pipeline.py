import dlt

from baliza import baliza_source


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="baliza_pipeline",
        destination='duckdb',
        dataset_name="baliza_data",
        progress="log",
        export_schema_path="schemas/export"
    )
    source = baliza_source()
    info = pipeline.run(source)
    print(info)
