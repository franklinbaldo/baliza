# Feature-Goal Alignment Matrix

This document tracks the alignment of BDD features with project goals.

## Summary

| Metric | Count |
|---|---|
| Total Feature Files | 8 |
| Total Scenarios | 20 |
| Last Updated | 2026-02-01 |

## Feature-Goal Mapping

| Feature File | Scenarios | Primary Goal | Status | Notes |
|---|---|---|---|---|
| `buffer_management.feature` | 4 | Operational Transparency | ✅ Implemented | Covers IA upload recording and buffer cleanup. |
| `checkpoint.feature` | 4 | Reliable Data Extraction | ✅ Implemented | Essential for resumability after failures. |
| `daily_export.feature` | 5 | Accessibility for Analysis | ✅ Implemented | Specialized daily package exports. |
| `end_to_end_extraction.feature` | 1 | Reliable Data Extraction | ⚠️ Quarantined | Skipped due to timeout issues in test env. |
| `export.feature` | 1 | Accessibility for Analysis | ✅ Implemented | Basic Parquet export functionality. |
| `resilience.feature` | 2 | Reliable Data Extraction | ❌ Not Implemented | Scenarios exist but step defs are empty/skipped. |
| `state_management.feature` | 3 | Operational Transparency | ⚠️ Partial | Mapped to `status` and `verify` commands. History is mocked/skipped. |
| `verification.feature` | 1 | Operational Transparency | ✅ Implemented | Gap detection via `verify` command. |

## Legend
- ✅ **Implemented**: Feature is fully functional and tested.
- ⚠️ **Partial/Quarantined**: Feature is partially implemented or tests are temporarily skipped.
- ❌ **Not Implemented**: Feature is planned (scenarios written) but code is missing.
