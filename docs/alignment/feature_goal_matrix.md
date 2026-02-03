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
| `buffer_management.feature` | 4 | Reliable Data Extraction | ✅ Implemented | Covers buffer cleanup and stats. |
| `checkpoint.feature` | 4 | Reliable Data Extraction | ✅ Implemented | Covers per-page checkpointing for resumability. |
| `daily_export.feature` | 5 | Accessibility for Analysis | ✅ Implemented | Covers daily self-contained parquet packages. |
| `end_to_end_extraction.feature` | 1 | Reliable Data Extraction | ✅ Implemented | Core pipeline test, now uses mock to avoid timeouts. |
| `export.feature` | 1 | Accessibility for Analysis | ✅ Implemented | Covers basic Parquet export. |
| `resilience.feature` | 2 | Reliable Data Extraction | ✅ Implemented | Fully implemented step definitions for retries. |
| `state_management.feature` | 3 | Reliable Data Extraction | ⚠️ Partial | `status` and `verify` aligned; `history` is xfailed. |
| `verification.feature` | 1 | Reliable Data Extraction | ✅ Implemented | Covers `verify` command for gap detection. |

## Gap Analysis

The current suite covers the core extraction and export goals well. Resilience testing and end-to-end extraction are now fully verified with mocks. State management is aligned with actual CLI commands, with unimplemented features clearly marked as expected to fail.

Priority for next steps:
1. Implement the `status --history` feature.
2. Consider adding more edge cases for large page extractions.
