# Feature-Goal Alignment Matrix

This document tracks the alignment of the BDD features in the codebase with the project's primary goals.

## Summary

| Metric | Count |
|---|---|
| Total Feature Files | 8 |
| Total Scenarios | ~12 |
| Last Updated | 2024-07-24 |

## Feature-Goal Mapping

| Feature File | Primary Goal | Status | Notes |
|---|---|---|---|
| `end_to_end_extraction.feature` | Reliable Data Extraction | ⚠️ Partially Implemented | Core pipeline works but autonomous mode is missing. Test currently skipped due to timeout. |
| `export.feature` | Accessibility for Analysis | ✅ Implemented | Basic export to Parquet is functional. |
| `resilience.feature` | Reliable Data Extraction | ⚠️ Partially Implemented | Retries are implemented in code, but run history tracking in tests is currently mocked. |
| `verification.feature` | Actionable Observability | ✅ Implemented | `verify` command exists and detects gaps. |
| `state_management.feature` | Actionable Observability | ❌ Mapped/Mocked | Tests "pass" by mapping to other commands or using mocks. Real `state` commands do not exist yet. |
| `checkpoint.feature` | Reliable Data Extraction | ✅ Implemented | Per-page checkpointing is implemented and used by `extract`. |
| `buffer_management.feature` | Data Preservation | ✅ Implemented | Logic for managing the local DuckDB buffer exists. |
| `daily_export.feature` | Accessibility for Analysis | ✅ Implemented | `export-daily` command generates self-contained packages. |

## Gap Analysis & Priorities

1.  **State Management (High Priority):** The `state` subcommand group (`show`, `gaps`, `history`) is documented in `README.md` and has BDD features, but is not yet implemented in the CLI. This is the immediate focus.
2.  **Autonomous Extraction:** Moving from manual `--start`/`--end` to a state-aware `extract` command that identifies what needs to be done.
3.  **Test Infrastructure:** Resolving the `pytest-httpx` timeout issues that cause several critical tests to be quarantined.
4.  **Endpoint Expansion:** Adding support for more PNCP endpoints beyond `contratos`.
