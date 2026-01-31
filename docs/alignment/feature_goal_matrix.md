# Feature-Goal Alignment Matrix

This document tracks the alignment of the BDD features in the codebase with the project's primary goals.

## Summary

| Metric | Count |
|---|---|
| Total Feature Files | 8 |
| Total Scenarios | 15 |
| Last Updated | 2024-07-24 |

## Feature-Goal Mapping

| Feature File | Scenarios | Primary Goal | Status | Notes |
|---|---|---|---|---|
| `buffer_management.feature` | 2 | Reliable Data Extraction | ✅ Implemented | Covers cleanup after IA upload and buffer statistics. |
| `checkpoint.feature` | 4 | Reliable Data Extraction | ✅ Implemented | Covers resumability via page-level checkpoints. |
| `daily_export.feature` | 5 | Accessibility for Analysis | ✅ Implemented | Covers relational export for Internet Archive. |
| `end_to_end_extraction.feature` | 1 | Reliable Data Extraction | ⚠️ Quarantined | Covers core pipeline. Quarantined due to CI timeout issues. |
| `export.feature` | 1 | Accessibility for Analysis | ✅ Implemented | Covers basic Parquet export. |
| `resilience.feature` | 1 | Reliable Data Extraction | ✅ Implemented | Covers retry logic and error handling. |
| `state_management.feature` | 3 | Reliable Data Extraction | ✅ Implemented | Fully aligned with the `baliza state` subcommand structure. |
| `verification.feature` | 1 | Reliable Data Extraction | ✅ Implemented | Covers gap detection via `baliza state gaps`. |

## Gap Analysis

The project's CLI and BDD test suite are now fully aligned. Core extraction, export, and state management features are implemented and tested. The primary remaining work is to expand endpoint coverage and resolve environmental issues affecting integration test stability.
