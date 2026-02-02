# Test Quarantine Log

This document tracks all tests marked as SKIP or XFAIL, in accordance with the project's quarantine policy.

## Quarantine Entry 1 (RESOLVED)

-   **Identifier:** `tests/step_defs/test_end_to_end_extraction.py::test_pipeline_is_resumable_and_idempotent`
-   **Status:** PASSED
-   **Reason:** Resolved by switching from `pytest-httpx` to `monkeypatch` for mocking, avoiding timeout issues.
-   **Resolved On:** 2026-02-12

## Quarantine Entry 2

-   **Identifier:** `tests/integration/test_pncp_real_api.py::test_pncp_extract_real_api_single_day`
-   **Status:** SKIP
-   **Reason:** Quarantined due to a persistent timeout issue, likely caused by live API calls in a CI environment.
-   **Reference:** Technical debt tracked in `docs/MASTERPLAN.md`.
-   **Added On:** 2024-07-24
-   **Expiry Date:** 2026-12-31
-   **Owner:** Baliza BDD Alignment Agent

## Quarantine Entry 3

-   **Identifier:** `tests/integration/test_pncp_real_api.py::test_pncp_pagination`
-   **Status:** SKIP
-   **Reason:** Quarantined due to a persistent timeout issue, likely caused by live API calls in a CI environment.
-   **Reference:** Technical debt tracked in `docs/MASTERPLAN.md`.
-   **Added On:** 2024-07-24
-   **Expiry Date:** 2026-12-31
-   **Owner:** Baliza BDD Alignment Agent

## Quarantine Entry 4

-   **Identifier:** `tests/integration/test_pncp_real_api.py::test_pncp_api_error_handling`
-   **Status:** SKIP
-   **Reason:** Quarantined due to a persistent timeout issue, likely caused by live API calls in a CI environment.
-   **Reference:** Technical debt tracked in `docs/MASTERPLAN.md`.
-   **Added On:** 2024-07-24
-   **Expiry Date:** 2026-12-31
-   **Owner:** Baliza BDD Alignment Agent

## Quarantine Entry 5

-   **Identifier:** `tests/integration/test_pncp_real_api.py::test_pncp_coverage_tracker`
-   **Status:** SKIP
-   **Reason:** Quarantined due to a persistent timeout issue, likely caused by live API calls in a CI environment.
-   **Reference:** Technical debt tracked in `docs/MASTERPLAN.md`.
-   **Added On:** 2024-07-24
-   **Expiry Date:** 2026-12-31
-   **Owner:** Baliza BDD Alignment Agent

## Quarantine Entry 6

-   **Identifier:** `tests/integration/test_pncp_api_simple.py`
-   **Status:** XFAIL
-   **Reason:** Contains scenarios that are expected to fail due to intentional API contract mismatches being tested.
-   **Added On:** 2026-02-12
-   **Expiry Date:** 2026-12-31
-   **Owner:** Baliza BDD Alignment Agent
