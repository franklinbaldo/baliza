# Feature-Goal Alignment Matrix (Corrected)

This document tracks the alignment of the *actual* BDD features in the codebase with the project's primary goals. It replaces a previous, aspirational version that described features not yet implemented.

## Summary

| Metric | Count |
|---|---|
| Total Feature Files | 8 |
| Total Scenarios | 21 |
| Last Updated | 2024-07-24 |

## Feature-Goal Mapping

| Feature File | Scenarios | Primary Goal | Status | Notes |
|---|---|---|---|---|
| `buffer_management.feature` | 4 | Data Preservation | ✅ Implemented | Covers data cleanup after IA upload and buffer statistics. |
| `checkpoint.feature` | 4 | Reliable Data Extraction | ✅ Implemented | Covers per-page checkpointing and resumption. |
| `daily_export.feature` | 5 | Accessibility for Analysis | ✅ Implemented | Covers the creation of daily self-contained data packages. |
| `end_to_end_extraction.feature` | 1 | Reliable Data Extraction | ✅ Implemented | Covers the core pipeline's ability to be resumable and idempotent. |
| `export.feature` | 1 | Accessibility for Analysis | ✅ Implemented | Covers the export of data to Parquet for consumption by other tools. |
| `resilience.feature` | 2 | Reliable Data Extraction | ✅ Implemented | Ensures the pipeline handles API errors gracefully, which is crucial for reliability. |
| `state_management.feature` | 3 | Reliable Data Extraction | ✅ Implemented | Covers the `state` command group (show, gaps, history). |
| `verification.feature` | 1 | Reliable Data Extraction | ✅ Implemented | Covers the `verify` command's ability to detect gaps, ensuring data completeness. |


## Gap Analysis

The BDD test suite is now fully aligned with the codebase and covers all primary commands and core logic. The initial gap between documentation and implementation has been closed.

The immediate priority is to maintain this alignment as new features are added and to ensure the `README.md` remains accurate.
