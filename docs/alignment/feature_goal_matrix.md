# Feature-Goal Alignment Matrix

This document tracks the alignment of BDD features with the project's primary goals.

## Summary

| Metric | Count |
|---|---|
| Total Feature Files | 8 |
| Implemented Scenarios | 4 |
| Aspirational Scenarios | 4 |
| Last Updated | 2024-07-25 |

## Feature-Goal Mapping

| Feature File | Scenarios | Primary Goal | Status | Notes |
|---|---|---|---|---|
| `end_to_end_extraction.feature` | 1 | Reliable Data Extraction | ✅ Implemented | Core resumable/idempotent pipeline. |
| `export.feature` | 1 | Accessibility for Analysis | ✅ Implemented | Parquet export for external consumption. |
| `resilience.feature` | 1 | Reliable Data Extraction | ✅ Implemented | Graceful handling of API errors. |
| `verification.feature` | 1 | Reliable Data Extraction | ✅ Implemented | `verify` command for gap detection. |
| `state_management.feature` | 1 | Reliable Data Extraction |  aspirational | Describes the `baliza state` commands. |
| `daily_export.feature` | 1 | Accessibility for Analysis |  aspirational | Automated daily export pipeline. |
| `checkpoint.feature` | 1 | Reliable Data Extraction |  aspirational | Advanced checkpointing during extraction. |
| `buffer_management.feature` | 1 | Reliable Data Extraction |  aspirational | In-memory buffer management. |
