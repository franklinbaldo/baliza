# 14. Adopt `dlt` for Data Extraction and Transformation

*   **Status**: Proposed
*   **Date**: 2025-07-23

## Context

Our current data pipeline relies on a custom-built, high-performance extractor (`AsyncPNCPExtractor`) and `dbt` for transformations. While powerful, this stack has several drawbacks:

*   **High Maintenance Overhead**: The custom extractor is complex and requires significant effort to maintain and extend.
*   **Complex Development Workflow**: The separation of extraction (Python) and transformation (`dbt`) creates a fragmented development workflow, making it harder to manage dependencies and ensure end-to-end data quality.
*   **Boilerplate and Redundancy**: `dbt` models for the bronze and silver layers often involve boilerplate code for schema definitions and basic transformations that could be automated.
*   **Steep Learning Curve**: The combination of a bespoke extractor and `dbt` presents a steep learning curve for new developers.

We are looking for a solution that simplifies our data pipeline, reduces maintenance overhead, and streamlines the development workflow.

## Decision

We will adopt the open-source library `dlt` (Data Load Tool) to replace our custom extractor and the `dbt` transformation layer for the bronze and silver stages.

`dlt` offers a unified, Python-native framework for both data extraction and transformation, with features that directly address our current challenges:

*   **Declarative REST API Source**: `dlt` provides a declarative way to define REST API sources, including handling pagination, authentication, and incremental loading. This will allow us to replace our complex `AsyncPNCPExtractor` with a much simpler and more maintainable `dlt` source definition.
*   **Automatic Schema Inference and Evolution**: `dlt` automatically infers schemas from the data and manages schema evolution over time. This will eliminate the need for most of our dbt models in the bronze and silver layers.
*   **Python-Native Transformations**: `dlt` allows us to define transformations directly in Python, which simplifies our stack and allows for better integration with our existing Python codebase.
*   **DuckDB Integration**: `dlt` has first-class support for DuckDB, which is already our primary data engine.

## Consequences

### Positive

*   **Simplified Codebase**: We will be able to remove the entire `dbt_baliza` project and the complex `AsyncPNCPExtractor`, significantly reducing the amount of code we need to maintain.
*   **Faster Development**: The simplified workflow and reduced boilerplate will allow us to develop new data pipelines and features more quickly.
*   **Improved Maintainability**: `dlt`'s declarative nature and automatic schema management will make our data pipelines easier to maintain and reason about.
*   **Lower Learning Curve**: A unified, Python-native framework will be easier for new developers to learn and contribute to.

### Negative

*   **Migration Effort**: We will need to invest time and effort to migrate our existing extraction and transformation logic to `dlt`.
*   **New Dependency**: We will be adding a new major dependency to our project, which comes with its own set of risks and maintenance considerations.

## Migration Plan

1.  **Create a new `dlt` pipeline in `src/baliza/pipeline.py`**: This pipeline will replicate the functionality of the `AsyncPNCPExtractor` using `dlt`'s REST API source.
2.  **Update the CLI**: The `src/baliza/cli.py` will be updated to include a new command for running the `dlt` pipeline.
3.  **Remove the old extractor and `dbt` project**: The `src/baliza/extractor.py` file and the `dbt_baliza` directory will be removed.
4.  **Update the loader**: The `src/baliza/loader.py` will be updated to read data from the `dlt`-managed database.
5.  **Update documentation**: The `README.md` and other relevant documentation will be updated to reflect the new architecture.
