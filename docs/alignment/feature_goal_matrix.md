# Feature-Goal Alignment Matrix (Corrected)

This document tracks the alignment of the *actual* BDD features in the codebase with the project's primary goals. It replaces a previous, aspirational version that described features not yet implemented.

## Summary

| Metric | Count |
|---|---|
| Total Feature Files | 8 |
| Total Scenarios | 21 |
| Last Updated | 2026-02-01 |

## Feature-Goal Mapping

| Feature File | Scenarios | Primary Goal | Status | Notes |
|---|---|---|---|---|
| `end_to_end_extraction.feature` | 1 | Reliable Data Extraction | ✅ Implemented | Covers the core pipeline's ability to be resumable and idempotent. Quarantined in CI. |
| `export.feature` | 1 | Accessibility for Analysis | ✅ Implemented | Covers the basic export of data to Parquet. |
| `resilience.feature` | 2 | Reliable Data Extraction | ✅ Implemented | Ensures the pipeline handles API errors gracefully and records run history. |
| `verification.feature` | 1 | Reliable Data Extraction | ✅ Implemented | Covers the `verify` command's ability to detect gaps. |
| `state_management.feature` | 3 | Resumability | ✅ Implemented | Covers \`state show\`, \`state gaps\`, and \`state history\`. |
| `buffer_management.feature` | 4 | Reliability | ✅ Implemented | Covers buffer statistics and cleanup. |
| `checkpoint.feature` | 4 | Resumability | ✅ Implemented | Covers per-page checkpointing. |
| `daily_export.feature` | 5 | Accessibility | ✅ Implemented | Covers structured daily packages. |


## Gap Analysis

The BDD test suite is now well-aligned with the core functionality. \`resilience.feature\` and \`state_management.feature\` have been fully implemented in this session, including the necessary CLI subcommands and run history tracking in \`PNCPExtractor\`.

The immediate priority is to stabilize the existing tests and ensure the project's documentation, starting with the `README.md`, accurately reflects the current, simpler architecture.
