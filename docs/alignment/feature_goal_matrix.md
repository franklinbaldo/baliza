# Feature-Goal Alignment Matrix

This document tracks the alignment of the BDD features in the codebase with the project's primary goals.

## Summary

| Metric | Count |
|---|---|
| Total Feature Files | 8 |
| Total Scenarios | 21 |
| Last Updated | 2024-07-24 |

## Feature-Goal Mapping

| Feature File | Scenarios | Primary Goal | Status | Notes |
|---|---|---|---|---|
| `end_to_end_extraction.feature` | 1 | Reliable Data Extraction | ⚠️ Quarantined | Skipped due to timeout/environment issues in CI. |
| `export.feature` | 1 | Accessibility for Analysis | ✅ Implemented | Covers basic Parquet export. |
| `daily_export.feature` | 1 | Accessibility for Analysis | ✅ Implemented | Covers daily package export (contratos + orgaos + unidades). |
| `resilience.feature` | 2 | Reliable Data Extraction | ❌ Placeholder | Scenarios defined but steps are mostly skipped (not implemented). |
| `verification.feature` | 1 | Data Quality Monitoring | ✅ Implemented | Covers `verify` command's gap detection. |
| `checkpoint.feature` | 4 | Reliable Data Extraction | ✅ Implemented | Verifies page-level checkpointing and resumability. |
| `state_management.feature` | 3 | Operator Experience | ⚠️ Partial | `show` and `gaps` are mapped/faked; `history` is skipped. |
| `buffer_management.feature` | 4 | Data Quality Monitoring | ✅ Implemented | Covers cleanup and stats of the local DuckDB buffer. |

## Gap Analysis

The BDD suite has grown but several key areas remain placeholder or faked:

1.  **State Management:** The `state` command doesn't actually exist in the CLI; tests map it to `status` and `verify`.
2.  **Resilience:** The resilience tests are not yet implemented with proper mocks/simulations.
3.  **Backfill:** No `backfill.feature` exists yet, although it's a Tier 1 goal.

Immediate priorities:
- Implement the `state` command to consolidate observability.
- Implement the `backfill` command for historical data.
- Fix resilience tests to ensure robustness.
