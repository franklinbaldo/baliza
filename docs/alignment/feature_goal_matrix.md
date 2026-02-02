# Feature-Goal Alignment Matrix (Updated)

This document tracks the alignment of the *actual* BDD features in the codebase with the project's primary goals.

## Summary

| Metric | Count |
|---|---|
| Total Feature Files | 8 |
| Total Scenarios | 21 |
| Last Updated | 2026-02-01 |

## Feature-Goal Mapping

| Feature File | Scenarios | Primary Goal | Status | Notes |
|---|---|---|---|---|
| `end_to_end_extraction.feature` | 1 | Reliable Data Extraction | ✅ Implemented | Covers the core pipeline's ability to be resumable and idempotent. |
| `export.feature` | 1 | Accessibility for Analysis | ✅ Implemented | Covers the export of data to Parquet for consumption by other tools. |
| `resilience.feature` | 2 | Reliable Data Extraction | ⚠️ Partial | Ensures the pipeline handles API errors gracefully. Step definitions are currently incomplete. |
| `verification.feature` | 1 | Reliable Data Extraction | ✅ Implemented | Covers the `verify` command's ability to detect gaps. |
| `buffer_management.feature` | 4 | Data Preservation | ✅ Implemented | Ensures buffer is cleaned after upload, maintaining manageable artifact size. |
| `checkpoint.feature` | 4 | Reliable Data Extraction | ✅ Implemented | Ensures progress is saved per page for resumability. |
| `daily_export.feature` | 5 | Accessibility for Analysis | ✅ Implemented | Covers the creation of daily self-contained Parquet packages. |
| `state_management.feature` | 3 | Reliable Data Extraction | ⚠️ Partial | Allows inspection of extraction state and history. Some scenarios are currently mocked or skipped. |


## Gap Analysis

The BDD suite has been expanded to cover more aspects of the project goals, including buffer management, checkpointing, and daily exports. The immediate priority is to complete the implementation of resilience tests and stabilize state management features.
