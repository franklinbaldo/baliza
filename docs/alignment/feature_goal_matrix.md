# Feature-Goal Alignment Matrix

This document tracks the alignment of the BDD features in the codebase with the project's primary goals.

## Summary

| Metric | Count |
|---|---|
| Total Feature Files | 8 |
| Total Scenarios | 13 |
| Last Updated | 2026-02-11 |

## Feature-Goal Mapping

| Feature File | Scenarios | Primary Goal | Status | Notes |
|---|---|---|---|---|
| `end_to_end_extraction.feature` | 1 | Reliable Data Extraction | ✅ Implemented | Covers resumability and idempotency. Stabilized via monkeypatch mocking. |
| `export.feature` | 1 | Accessibility for Analysis | ✅ Implemented | Covers the `export` command. |
| `resilience.feature` | 2 | Reliable Data Extraction | ✅ Implemented | Scenarios and step definitions implemented. Some parts XFAIL due to missing run history tracking. |
| `verification.feature` | 1 | Reliable Data Extraction | ✅ Implemented | Covers the `verify` command's gap detection. |
| `buffer_management.feature` | 4 | Reliable Data Extraction | ✅ Implemented | Covers `buffer-stats` and cleanup. |
| `checkpoint.feature` | 4 | Reliable Data Extraction | ✅ Implemented | Covers internal checkpointing logic. |
| `daily_export.feature` | 5 | Accessibility for Analysis | ✅ Implemented | Covers `export-daily` command. |
| `state_management.feature` | 3 | Reliable Data Extraction | ❌ Not Implemented | Scenarios/steps exist but XFAIL because CLI `state` subcommands are missing. |

## Gap Analysis

The project has made significant progress in implementing core extraction and export features. The `end_to_end_extraction` test has been stabilized. Resilience tests are now implemented but depend on future implementation of run history tracking. The `state` commands remain the primary missing feature in the CLI.

Immediate priorities are to implement the `state` subcommands and integrate run history tracking into the `PNCPExtractor`.
