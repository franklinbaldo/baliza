# BDD Alignment Actions

This document records the actions taken to align the BDD features with the project goals.

## Session 1: Idempotency and Resumability

**Date:** 2024-05-22

**Changes:**

1.  **Fixed Skipped Test:** The `end_to_end_extraction.feature` scenario was quarantined due to persistent timeouts with `pytest-httpx`.
    -   **Action:** Replaced `pytest-httpx` with `pytest.monkeypatch` for more reliable mocking of `httpx.Client.get`.
    -   **Reason:** `pytest-httpx` can be unreliable when testing CLI applications, and `monkeypatch` provides a more direct and stable way to mock network requests.
2.  **Improved BDD Clarity:** The `end_to_end_extraction.feature` was renamed and refactored to better reflect its purpose.
    -   **Action:** Renamed the feature to `idempotency.feature` and updated the scenario to focus exclusively on idempotency.
    -   **Reason:** The original scenario was misnamed, as it only tested idempotency, not resumability.
3.  **Added Automatic Retry Test:** A new BDD feature and step definition were created to explicitly test the pipeline's automatic retry behavior.
    -   **Action:** Created `automatic_retry.feature` and `test_automatic_retry.py` to simulate a transient API error and verify that the pipeline can recover.
    -   **Reason:** The application's built-in retry logic is a critical feature that was not being tested. The initial resumability test was flawed because it did not account for this behavior.
