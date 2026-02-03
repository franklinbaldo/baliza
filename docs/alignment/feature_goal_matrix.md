# Feature-Goal Alignment Matrix

This document tracks the alignment of the actual BDD features in the codebase with the project's primary goals.

## Summary

| Metric | Count |
|---|---|
| Total Feature Files | 8 |
| Total Scenarios | 21 |
| Last Updated | 2026-02-11 |

## Feature-Goal Mapping

| Feature File | Scenarios | Primary Goal | Status | Notes |
|---|---|---|---|---|
| `end_to_end_extraction.feature` | 1 | Reliable Data Extraction | ✅ Implemented | Core pipeline E2E test. |
| `checkpoint.feature` | 4 | Reliable Data Extraction | ✅ Implemented | Tests resumability via checkpoints. |
| `resilience.feature` | 3 | Reliable Data Extraction | ✅ Implemented | Tests retry logic and error handling. |
| `state_management.feature` | 3 | Reliable Data Extraction | ⚠️ Faked in Tests | CLI subcommands `state show`, `gaps`, `history` are NOT implemented; tests use mocks. |
| `verification.feature` | 1 | Reliable Data Extraction | ✅ Implemented | Tests `verify` command for gap detection. |
| `buffer_management.feature` | 4 | Data Preservation | ✅ Implemented | Tests local staging (DuckDB) and IA upload tracking. |
| `export.feature` | 1 | Accessibility for Analysis | ✅ Implemented | Tests basic `export` command to Parquet. |
| `daily_export.feature` | 4 | Accessibility for Analysis | ✅ Implemented | Tests `export-daily` package generation. |

## Gap Analysis

The project has good coverage of core extraction and export logic. However, there is a significant gap in CLI-level state management observability:
- `state` subcommands (`show`, `gaps`, `history`) are documented but not implemented in `cli_simple.py`.
- `backfill` and `tiers` commands are missing from the CLI.
- The `extractor.py` is currently hardcoded for the `contratos` resource, hindering the goal of "Comprehensive Endpoint Coverage".
- Some BDD tests (specifically `state_management.feature`) are currently using mocks or mapping to other commands to pass CI, hiding the implementation gaps.
