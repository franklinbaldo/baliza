# Feature-Goal Alignment Matrix

This document tracks the alignment of the BDD features in the codebase with the project's primary goals.

## Summary

| Metric | Count |
|---|---|
| Total Feature Files | 8 |
| Total Scenarios | 21 |
| Last Updated | 2026-02-01 |

## Feature-Goal Mapping

| Feature File | Scenarios | Primary Goal | Status | Notes |
|---|---|---|---|---|
| `buffer_management.feature` | 4 | Reliable Data Extraction | ✅ Implemented | Covers buffer cleanup, unstable data detection, and stats. |
| `checkpoint.feature` | 4 | Reliable Data Extraction | ✅ Implemented | Covers per-page checkpointing and resumability on failure. |
| `daily_export.feature` | 5 | Accessibility for Analysis | ✅ Implemented | Covers creation of daily Parquet packages with metadata. |
| `end_to_end_extraction.feature` | 1 | Reliable Data Extraction | ⚠️ Quarantined | Covers E2E resumability and idempotency. Currently skipped due to timeout issues. |
| `export.feature` | 1 | Accessibility for Analysis | ✅ Implemented | Covers basic export of DuckDB tables to Parquet. |
| `resilience.feature` | 2 | Reliable Data Extraction | ✅ Implemented | Covers recovery from transient API errors and handling of fatal errors. |
| `state_management.feature` | 3 | Reliable Data Extraction | ⚠️ Partial | Covers show/gaps/history. *Note: tests currently map these to other commands or mock them.* |
| `verification.feature` | 1 | Reliable Data Extraction | ✅ Implemented | Covers gap detection in extracted data. |

## Gap Analysis

The project has achieved good coverage of its core goals through BDD features. However, there is a mismatch in the `state_management.feature` where the CLI has not yet implemented the `state` command group, and tests are using workarounds.

The immediate priority is to align the CLI implementation with the BDD scenarios and the documentation (specifically the `state` and `backfill` commands).
