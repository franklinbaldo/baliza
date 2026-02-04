# Feature-Goal Alignment Matrix

This document tracks the alignment of all BDD features in the codebase with the project's primary goals.

## Summary

| Metric | Count |
|---|---|
| Total Feature Files | 8 |
| Total Scenarios | 21 |
| Last Updated | 2026-02-11 |

## Feature-Goal Mapping

| Feature File | Scenarios | Primary Goal | Tier | Status | Notes |
|---|---|---|---|---|---|
| `buffer_management.feature` | 4 | Reliable Data Extraction | Tier 1 | ✅ Implemented | Manages the "bronze" layer storage and cleanup. |
| `checkpoint.feature` | 4 | Reliable Data Extraction | Tier 0 | ✅ Implemented | Essential for resumability and preventing data loss. |
| `daily_export.feature` | 5 | Accessibility for Analysis | Tier 2 | ✅ Implemented | Creates self-contained daily data packages. |
| `end_to_end_extraction.feature` | 1 | Reliable Data Extraction | Tier 0 | ⚠️ Quarantined | Full pipeline verification. Currently skipped due to CI timeout. |
| `export.feature` | 1 | Accessibility for Analysis | Tier 0 | ✅ Implemented | Basic Parquet export functionality. |
| `resilience.feature` | 2 | Reliable Data Extraction | Tier 1 | ❌ Not Implemented | Covers error recovery and retry logic. Scenarios exist but steps are mocked/skipped. |
| `state_management.feature` | 3 | Reliable Data Extraction | Tier 1 | ⚠️ Partial / Mocked | CLI subcommands `state show`, `state gaps`, `state history` are missing or mocked. |
| `verification.feature` | 1 | Reliable Data Extraction | Tier 1 | ✅ Implemented | Detects gaps in extracted data using the `verify` command. |

## Tier Distribution

| Tier | Feature Files | Status |
|---|---|---|
| **Tier 0: Critical Path** | 3 | ✅ Core functionality stable. |
| **Tier 1: Core Features** | 4 | ⚠️ Significant gaps in resilience and state management. |
| **Tier 2: Operator Experience** | 1 | ✅ Daily export is stable. |
| **Tier 3: Future Enhancements** | 0 | ⚪ None currently in the BDD suite. |

## Action Plan

1.  **Stabilize Resilience Tests**: Implement the missing steps in `test_resilience.py` using `monkeypatch` to simulate API failures.
2.  **Align State Management**: Either implement the `state` subcommands in the CLI or update the feature to use existing `status` and `verify` commands.
3.  **Address E2E Timeout**: Resolve the timeout issue in `test_end_to_end_extraction.py` to move it out of quarantine.
