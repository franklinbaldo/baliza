# Feature-Goal Alignment Matrix

This document tracks the alignment of BDD features with the project's primary goals.

**Last Updated:** 2024-07-24

## Summary

This matrix reflects the state of the BDD features after a major documentation and code audit. The previous version was aspirational and did not match the simpler, implemented reality. The current features map to a more focused set of goals.

## Primary Goals

- **G1: Reliable Data Extraction:** Reliably extract public procurement data from the PNCP.
- **G2: Data Preservation:** Create a preserved, long-term archive of the data.
- **G3: Data Accessibility:** Make the data accessible for analysis.

## Feature-Goal Mapping

| Feature File | Scenarios | Primary Goal | Status | Notes |
|---|---|---|---|---|
| `end_to_end_extraction.feature` | TBD | G1 | ⚠️ Needs Update | Scenarios must be updated to reflect the simple `extract` command (no resumability). |
| `export.feature` | TBD | G3 | ⚠️ Needs Update | Scenarios must be aligned with the simple `export` and `export-daily` commands. |
| `verification.feature` | TBD | G1, G2 | ⚠️ Needs Update | Scenarios must be aligned with the simple `verify` command. |
| `checkpoint.feature`| TBD | G1 | ✅ Aligned | Tests the per-page checkpointing mechanism. |
| `resilience.feature` | TBD | G1 | ✅ Aligned | Tests the retry logic in the extractor. |
| `buffer_management.feature` | TBD | G2 | ⚠️ Needs Update | Scenarios relate to buffer stats and cleanup; need verification. |
| `daily_export.feature` | TBD | G3 | ✅ Aligned | Tests the self-contained daily export package. |

## Status Key

- ✅ **Aligned:** Feature file accurately reflects the current implementation.
- ⚠️ **Needs Update:** Feature file contains scenarios that are out of sync with the code.
- ❌ **Not Implemented:** Feature file describes behavior that does not exist.
