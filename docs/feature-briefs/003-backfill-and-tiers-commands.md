# Feature Brief: Backfill and Tiers Commands

-   **ID:** 003
-   **Epic:** Resumable Extraction Pipeline / Developer Experience
-   **Goal Mapping:** Excellent Developer/Operator Experience, Comprehensive Endpoint Coverage
-   **Status:** Ready for Implementation

## 1. User Story

As a developer or operator, I want a `backfill` command to easily extract large historical ranges, and a `tiers` command to understand which features are stable and which are experimental.

## 2. Acceptance Criteria (BDD-Style)

### Scenario: Backfill a month of data
- **Given** I want to extract all data for January 2024
- **When** I run `baliza backfill --month 2024-01`
- **Then** the command should automatically calculate the start and end dates for that month
- **And** it should execute the extraction logic for that range.

### Scenario: View feature tiers
- **Given** I am a new user of Baliza
- **When** I run `baliza tiers`
- **Then** I should see a list of all CLI commands organized by Tier (Tier 0 to Tier 3)
- **And** each command should have a status indicator (Done, In Progress, Planned).

## 3. Implementation Notes

-   **Backfill:** Should support `--month` (YYYY-MM) and possibly `--year` (YYYY) formats. It should delegate the actual extraction to the existing `extractor.py` logic.
-   **Tiers:** This command should read the classification from a central place (e.g., `src/baliza/tiers.py`) and display it using a Rich table as described in `docs/alignment/feature-hierarchy.md`.
-   **Refactoring:** Ensure the `@tierN` decorators are correctly applied to all commands in `cli_simple.py`.

## 4. Out-of-Scope

-   Parallelizing the backfill (should be sequential for now to avoid API rate limits).
-   Interactive tier selection.
