# Feature-Goal Alignment Matrix (Corrected)

This document tracks the alignment of the *actual* BDD features in the codebase with the project's primary goals. It replaces a previous, aspirational version that described features not yet implemented.

## Summary

| Metric | Count |
|---|---|
| Total Feature Files | 4 |
| Total Scenarios | 4 |
| Last Updated | 2024-07-24 |

## Feature-Goal Mapping

| Feature File | Scenarios | Primary Goal | Status | Notes |
|---|---|---|---|---|
| `end_to_end_extraction.feature` | 1 | Reliable Data Extraction | ✅ Implemented | Covers the core pipeline's ability to be resumable and idempotent. Test is currently quarantined due to a timeout issue. |
| `export.feature` | 1 | Accessibility for Analysis | ✅ Implemented | Covers the export of data to Parquet for consumption by other tools. |
| `resilience.feature` | 1 | Reliable Data Extraction | ✅ Implemented | Ensures the pipeline handles API errors gracefully, which is crucial for reliability. |
| `verification.feature` | 1 | Reliable Data Extraction | ✅ Implemented | Covers the `verify` command's ability to detect gaps, ensuring data completeness. |


## Gap Analysis

This matrix reflects the *current* state of the BDD test suite. The previous version of this document described a much larger, aspirational suite including features like `state_management.feature`, `backfill.feature`, and `data_quality.feature`. These features do not currently exist and represent a significant gap between the project's documentation and its implementation.

The immediate priority is to stabilize the existing tests and ensure the project's documentation, starting with the `README.md`, accurately reflects the current, simpler architecture.
