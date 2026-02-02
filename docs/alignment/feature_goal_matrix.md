# Feature-Goal Alignment Matrix

This document tracks the alignment of the BDD features in the codebase with the project's primary goals.

## Summary

| Metric | Count |
|---|---|
| Total Feature Files | 8 |
| Total Scenarios | 21 |
| Last Updated | 2026-02-11 |

## Feature-Goal Mapping

| Feature File | Scenarios | Primary Goal | Status | Notes |
|---|---|---|---|---|
| `end_to_end_extraction.feature` | 1 | Reliable Data Extraction | ✅ Implemented | Core pipeline resumability. |
| `checkpoint.feature` | 4 | Reliable Data Extraction | ✅ Implemented | Per-page checkpointing logic. |
| `resilience.feature` | 2 | Reliable Data Extraction | ✅ Implemented | Retry logic and failure handling. |
| `verification.feature` | 1 | Data Quality Monitoring | ✅ Implemented | Gap detection in coverage. |
| `state_management.feature` | 3 | Data Quality Monitoring | ⚠️ Partial | CLI commands for status and history. Some scenarios mocked or skipped. |
| `buffer_management.feature` | 4 | Data Preservation | ✅ Implemented | Managing the local DuckDB buffer. |
| `export.feature` | 1 | Data Accessibility | ✅ Implemented | Basic Parquet export. |
| `daily_export.feature` | 5 | Data Accessibility | ✅ Implemented | Daily partitioned export packages. |

## Gap Analysis

The project has good coverage for core extraction and export functionality. The following gaps have been identified:

1. **Backfill Feature:** There is no BDD specification for the `backfill` command, which is a Tier 1 requirement.
2. **State Management Completeness:** The `state history` command is not yet fully implemented and is currently skipped in tests.
3. **Data Quality Checks:** Scenarios for more advanced data validation (schema checks, deduplication logic) are needed.
4. **Configuration:** BDD specs for pipeline configuration are missing.

## Next Actions

1. Create `backfill.feature` to specify historical processing.
2. Complete implementation of `state history` and update its BDD steps.
3. Expand `verification.feature` to include more detailed audit scenarios.
