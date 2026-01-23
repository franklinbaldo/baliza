# Test Quarantine Registry

This document lists all tests that are currently under a `skip` or `xfail` quarantine.

## Principles

-   **Skip**: Used when a test cannot be run due to environmental constraints (e.g., unavailable external service).
-   **XFail**: Used for known, reproducible bugs or features that are not yet implemented.
-   **Traceability**: Every quarantined test must have a clear reason, a reference, and an expiry date.

---

## Quarantined Tests

### 1. `tests/integration/test_pncp_real_api.py`

-   **Test**: `test_pncp_extract_real_api_single_day`
-   **Quarantine Type**: `skip`
-   **Reason**: The test consistently times out in the CI environment. This test makes a live call to an external API, which is both slow and unreliable in a CI context.
-   **Reference**: `docs/MASTERPLAN.md` (under "Technical Debt")
-   **Added on**: 2024-05-20
-   **Expiry**: 2024-06-20 (or when the test is rewritten to use mocks)
-   **Owner**: Baliza BDD Alignment & Improvement Agent
-   **Action**: The test is temporarily skipped to unblock the CI pipeline. It should be rewritten to use `pytest-httpx` to mock the API responses instead of making live network calls.
