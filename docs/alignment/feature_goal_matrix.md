# Feature-Goal Alignment Matrix

This document tracks the alignment of the *actual* BDD features in the codebase with the project's primary goals.

## Summary

| Metric | Count |
|---|---|
| Total Feature Files | 8 |
| Total Scenarios | 21 |
| Last Updated | 2026-02-12 |

## Feature-Goal Mapping

| Feature File | Scenarios | Primary Goal | Status | Notes |
|---|---|---|---|---|
| `buffer_management.feature` | 4 | Reliable Data Extraction | ✅ Implemented | Covers data stability and cleanup after upload. |
| `checkpoint.feature` | 4 | Reliable Data Extraction | ✅ Implemented | Covers page-level checkpointing and resumption. |
| `daily_export.feature` | 5 | Data Preservation | ✅ Implemented | Covers creation of daily Parquet packages with metadata. |
| `end_to_end_extraction.feature` | 1 | Reliable Data Extraction | ⚠️ Quarantined | Covers resumability/idempotency. Skipped due to timeout issues. |
| `export.feature` | 1 | Accessibility for Analysis | ✅ Implemented | Covers basic Parquet export. |
| `resilience.feature` | 2 | Reliable Data Extraction | ❌ Not Implemented | Defined but step definitions are currently skipped. |
| `state_management.feature` | 3 | Operator Experience | ⚠️ Partial | Covers show, gaps, and history. History is currently mocked/skipped. |
| `verification.feature` | 1 | Data Quality Monitoring | ✅ Implemented | Covers gap detection in coverage. |

## Gap Analysis

The project has a solid foundation of BDD features, but some critical areas are currently skipped or not fully implemented:
1. **Resilience:** The error recovery logic is not yet verified by automated BDD tests.
2. **State Management:** The `history` command is not yet implemented in the CLI.
3. **E2E Stability:** The core resumability test is currently quarantined.

The immediate priority is to stabilize the E2E tests and implement the missing CLI commands defined in the feature briefs.
