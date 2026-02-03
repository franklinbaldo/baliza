# Feature-Goal Alignment Matrix

This document tracks the alignment of the BDD features in the codebase with the project's primary goals.

## Summary

| Metric | Count |
|---|---|
| Total Feature Files | 8 |
| Total Scenarios | 21 |
| Last Updated | 2026-02-03 |

## Feature-Goal Mapping

| Feature File | Scenarios | Primary Goal | Status | Notes |
|---|---|---|---|---|
| `end_to_end_extraction.feature` | 1 | Reliable Data Extraction | ✅ Implemented | Covers the core pipeline's ability to be resumable and idempotent. Stabilized in Feb 2026. |
| `export.feature` | 1 | Accessibility for Analysis | ✅ Implemented | Covers basic Parquet export. |
| `resilience.feature` | 2 | Reliable Data Extraction | ✅ Implemented | Ensures the pipeline handles API errors and retries gracefully. |
| `verification.feature` | 1 | Reliable Data Extraction | ✅ Implemented | Covers the `verify` command's ability to detect gaps. |
| `state_management.feature` | 3 | Operator Experience | ✅ Implemented | Covers `state show`, `state gaps`, and `state history` commands. |
| `checkpoint.feature` | 4 | Reliable Data Extraction | ✅ Implemented | Ensures per-page checkpointing and resumability. |
| `buffer_management.feature` | 4 | Data Preservation | ✅ Implemented | Manages recent unstable data and cleanup after upload. |
| `daily_export.feature` | 5 | Accessibility for Analysis | ✅ Implemented | Covers creation of daily self-contained data packages. |

## Status Assessment

The BDD suite now fully covers the core functionality described in the README.md, including the recently implemented state management and resilience features. The tests have been stabilized by moving away from flaky mocking libraries where appropriate.
