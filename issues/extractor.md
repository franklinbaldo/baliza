## `extractor.py`: Simplified Reconciliation Logic and Architectural Clarity

### Problem

The `AsyncPNCPExtractor` in `src/baliza/extractor.py` has two main issues:

1.  **Simplified Reconciliation Logic**: The `_expand_and_reconcile_from_db_vectorized` method uses hardcoded, simplified Pandas DataFrames for the discovery and existing data. The core logic of querying the database to get the actual state of the extraction is not implemented. This is a critical part of the two-pass reconciliation architecture.

2.  **Architectural Clarity**: The application seems to have two different extraction mechanisms:
    *   The `AsyncPNCPExtractor` in this file, which uses a vectorized, two-pass approach.
    *   The `ExtractionCoordinator`, which orchestrates a "dbt-driven" extraction process.

    It's not clear how these two mechanisms relate to each other. The `AsyncPNCPExtractor` does not implement the `extract_dbt_driven` method from the `ExtractorProtocol`, which adds to the confusion. The CLI's `extract` command seems to use `AsyncPNCPExtractor`, but other parts of the system might be using the dbt-driven approach.

### Potential Solutions

1.  **Implement the Reconciliation Logic**:
    *   The simplified queries in `_expand_and_reconcile_from_db_vectorized` need to be replaced with actual DuckDB queries that:
        *   Fetch the results of the discovery pass (page 1 responses).
        *   Fetch all existing request metadata from the database.
        *   Perform the anti-join to find the missing pages.

2.  **Clarify the Architecture**:
    *   The relationship between the vectorized extractor and the dbt-driven extractor needs to be clarified. Are they two alternative implementations? Is one legacy and the other new?
    *   The documentation should explain when to use each one.
    *   If the vectorized extractor is the new standard, it should probably implement the `extract_dbt_driven` method, or the `ExtractorProtocol` should be updated.
    *   If the dbt-driven approach is preferred, then the role of the `AsyncPNCPExtractor` needs to be defined. Perhaps it's a lower-level component used by the `ExtractionCoordinator`.

### Recommendation

First, implement the database queries in `_expand_and_reconcile_from_db_vectorized` to make the vectorized extractor fully functional.

Second, clarify the architectural role of the `AsyncPNCPExtractor` versus the `ExtractionCoordinator`. The ideal solution would be to unify them, so that the `ExtractionCoordinator` uses the efficient, vectorized reconciliation logic from the `AsyncPNCPExtractor` as part of its execution phase. This would combine the best of both worlds: the clear orchestration of the coordinator and the high performance of the vectorized extractor.
