# Feature-Goal Alignment Matrix

This document tracks the alignment of the *actual* BDD features in the codebase with the project's primary goals.

## Summary

| Metric | Count |
|---|---|
| Total Feature Files | 8 |
| Total Scenarios | 21 |
| Last Updated | 2026-01-31 |

## Feature-Goal Mapping

| Feature File | Scenarios | Primary Goal | Status | Notes |
|---|---|---|---|---|
| `end_to_end_extraction.feature` | 1 | Reliable Data Extraction | ⚠️ Quarantined | Covers the core pipeline's ability to be resumable and idempotent. Test is currently quarantined due to a timeout issue. |
| `export.feature` | 1 | Accessibility for Analysis | ✅ Implemented | Covers the basic export of data to Parquet. |
| `resilience.feature` | 2 | Reliable Data Extraction | ⚠️ Quarantined | Ensures the pipeline handles API errors gracefully. Quarantined. |
| `verification.feature` | 1 | Reliable Data Extraction | ✅ Implemented | Covers the `verify` command's ability to detect gaps. |
| `buffer_management.feature` | 4 | Buffer Management & Resumability | ✅ Implemented | Tracks rows in buffer and cleanup after IA upload. |
| `checkpoint.feature` | 4 | Buffer Management & Resumability | ✅ Implemented | Ensures extraction can resume from the last successful page. |
| `daily_export.feature` | 5 | Daily Export Packages | ✅ Implemented | Validates the creation of self-contained daily Parquet packages. |
| `state_management.feature` | 3 | Data Quality Monitoring | 🟠 Partially Implemented | Maps to `status` and `verify` commands. `history` scenario is currently skipped. |

## Gap Analysis

The current feature set covers the core extraction, resilience, and export goals. However, the CLI commands for state management are currently split between `status` and `verify`, while the BDD features use a more unified `state` command structure that is mapped to these existing commands in the step definitions.

The immediate priority is to:
1.  Unify the state management CLI commands under a `state` subcommand group.
2.  Implement the Tier system in code as documented in `feature-hierarchy.md`.
3.  Fix the quarantined tests to ensure reliable CI.
