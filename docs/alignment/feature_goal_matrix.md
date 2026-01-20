# Feature-Goal Alignment Matrix (Re-baselined)

This document has been regenerated based on an analysis of the existing `.feature` files in the repository. It supersedes the previous, inaccurate version.

## Summary

The current BDD tests cover a minimal, functional core for the Baliza CLI but lack the advanced features described in previous documentation. The foundation is solid, but features for resumability, state management, and historical backfill are missing.

| Metric | Count |
|--------|-------|
| Total Feature Files | 4 |
| Total Scenarios | 5 |
| Last Updated | 2026-01-20 |

## Feature-Goal Mapping

| Feature File | Scenarios | Primary Goal | Status | Notes |
|---|---|---|---|---|
| `extraction.feature` | 2 | Reliable Data Extraction | ⚠️ **Partial** | Covers basic daily extraction. Lacks state management, resumability, and error recovery. |
| `export.feature` | 1 | Data Accessibility | ⚠️ **Partial** | Covers basic Parquet export. Lacks partitioning, filtering, and automation. |
| `resilience.feature`| 1 | Reliable Data Extraction | ⚠️ **Partial** | Covers basic API error handling for a single request. Lacks retry logic or complex failure modes. |
| `verification.feature`| 1 | Data Preservation | ⚠️ **Partial** | Covers simple gap detection. Lacks automated reporting or integration with state management. |

## Gap Analysis & Next Steps

The most significant finding is the absence of features that were previously documented as complete. The following features do **not** currently exist and should be prioritized for implementation:

1.  **State Management (`state_management.feature`):**
    *   **Description:** The ability to track which date ranges have been successfully extracted to prevent redundant work and data duplication. This is critical for a reliable, resumable pipeline.
    *   **Priority:** High. This is the top priority for closing the gap between documentation and reality.

2.  **Backfill (`backfill.feature`):**
    *   **Description:** A dedicated workflow for efficiently extracting large amounts of historical data (e.g., entire months or years).
    *   **Priority:** Medium. Essential for achieving the goal of data preservation.

3.  **Data Quality (`data_quality.feature`):**
    *   **Description:** Scenarios for validating the schema, integrity, and correctness of the extracted data beyond simple gap detection.
    *   **Priority:** Medium. Important for ensuring the data is trustworthy for analysis.

**Recommendation:** The next development cycle should focus entirely on implementing a `state_management.feature` to create a truly resumable extraction pipeline.
