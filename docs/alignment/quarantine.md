# Test Quarantine Log

This document tracks all tests marked as SKIP or XFAIL, in accordance with the project's quarantine policy.

## Quarantine Entry 2

-   **Identifier:** `tests/integration/test_pncp_real_api.py::test_pncp_extract_real_api_single_day`
-   **Status:** SKIP
-   **Reason:** Quarantined due to a persistent timeout issue, likely caused by live API calls in a CI environment.
-   **Reference:** Technical debt tracked in `docs/MASTERPLAN.md`.
-   **Added On:** 2024-07-24
-   **Expiry Date:** 2024-10-24
-   **Owner:** Baliza BDD Feature Builder

## Quarantine Entry 3

-   **Identifier:** `tests/integration/test_pncp_real_api.py::test_pncp_pagination`
-   **Status:** SKIP
-   **Reason:** Quarantined due to a persistent timeout issue, likely caused by live API calls in a CI environment.
-   **Reference:** Technical debt tracked in `docs/MASTERPLAN.md`.
-   **Added On:** 2024-07-24
-   **Expiry Date:** 2024-10-24
-   **Owner:** Baliza BDD Feature Builder

## Quarantine Entry 4

-   **Identifier:** `tests/integration/test_pncp_real_api.py::test_pncp_api_error_handling`
-   **Status:** SKIP
-   **Reason:** Quarantined due to a persistent timeout issue, likely caused by live API calls in a CI environment.
-   **Reference:** Technical debt tracked in `docs/MASTERPLAN.md`.
-   **Added On:** 2024-07-24
-   **Expiry Date:** 2024-10-24
-   **Owner:** Baliza BDD Feature Builder

## Quarantine Entry 5

-   **Identifier:** `tests/integration/test_pncp_real_api.py::test_pncp_coverage_tracker`
-   **Status:** SKIP
-   **Reason:** Quarantined due to a persistent timeout issue, likely caused by live API calls in a CI environment.
-   **Reference:** Technical debt tracked in `docs/MASTERPLAN.md`.
-   **Added On:** 2024-07-24
-   **Expiry Date:** 2024-10-24
-   **Owner:** Baliza BDD Feature Builder

## Quarantine Entry 6

-   **Identifier:** `tests/step_defs/test_resilience.py::test_extract_recovers_from_transient_error`
-   **Status:** XFAIL
-   **Reason:** Fails because run history tracking in `baliza_state.runs` is not yet implemented in `PNCPExtractor.extract`.
-   **Reference:** Feature Brief 001, Epic 1.
-   **Added On:** 2026-02-11
-   **Expiry Date:** 2026-05-11
-   **Owner:** Baliza BDD Feature Builder

## Quarantine Entry 7

-   **Identifier:** `tests/step_defs/test_resilience.py::test_extract_fails_after_multiple_retries`
-   **Status:** XFAIL
-   **Reason:** Fails because state history logging for failed attempts is not yet implemented.
-   **Reference:** Feature Brief 001, Epic 1.
-   **Added On:** 2026-02-11
-   **Expiry Date:** 2026-05-11
-   **Owner:** Baliza BDD Feature Builder

## Quarantine Entry 8

-   **Identifier:** `tests/step_defs/test_state_management.py::test_show_state`
-   **Status:** XFAIL
-   **Reason:** CLI command `baliza state show` is not yet implemented.
-   **Reference:** Feature Brief 001, Epic 1.
-   **Added On:** 2026-02-11
-   **Expiry Date:** 2026-05-11
-   **Owner:** Baliza BDD Feature Builder

## Quarantine Entry 9

-   **Identifier:** `tests/step_defs/test_state_management.py::test_list_gaps`
-   **Status:** XFAIL
-   **Reason:** CLI command `baliza state gaps` is not yet implemented.
-   **Reference:** Feature Brief 001, Epic 1.
-   **Added On:** 2026-02-11
-   **Expiry Date:** 2026-05-11
-   **Owner:** Baliza BDD Feature Builder

## Quarantine Entry 10

-   **Identifier:** `tests/step_defs/test_state_management.py::test_show_history`
-   **Status:** XFAIL
-   **Reason:** CLI command `baliza state history` is not yet implemented.
-   **Reference:** Feature Brief 001, Epic 1.
-   **Added On:** 2026-02-11
-   **Expiry Date:** 2026-05-11
-   **Owner:** Baliza BDD Feature Builder
