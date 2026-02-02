# Feature Brief: Test Stabilization (E2E & Resilience)

-   **ID:** 004
-   **Epic:** CLI Maturity & Observability
-   **Goal Mapping:** Achieve Full Extraction Resumability
-   **Status:** Ready for Implementation

## 1. User Story

As a developer, I want the project's BDD tests to be stable and pass in all environments, so that I can confidently make changes without fear of regressions or being blocked by flaky infrastructure.

## 2. Acceptance Criteria

### Task 1: Stabilize E2E Extraction Test
- **File:** `tests/step_defs/test_end_to_end_extraction.py`
- **Goal:** Unskip `test_pipeline_is_resumable_and_idempotent`.
- **Action:** Replace `pytest-httpx` with a custom `monkeypatch` of `httpx.Client.get` (or similar) to mock the PNCP API. This avoids the persistent timeout issues associated with the `pytest-httpx` plugin in some environments.

### Task 2: Implement Resilience Step Definitions
- **File:** `tests/step_defs/test_resilience.py`
- **Goal:** Implement all steps that currently have `pytest.skip("Not implemented")`.
- **Action:** Use a stateful mock API (via `monkeypatch`) that can simulate:
    - Transient 500 errors (failing once, then succeeding).
    - Persistent 500 errors (failing multiple times).
- **Verification:** Ensure `test_extract_recovers_from_transient_error` and `test_extract_fails_after_multiple_retries` pass.

### Task 3: Tier Tagging
- Ensure all tests are correctly tagged with `@tier0`, `@tier1`, etc., as per the hierarchy defined in `docs/alignment/bdd-feature-plan.md`.

## 3. Implementation Notes
- Use `monkeypatch` to override `baliza.extractor._fetch_page` or `httpx.Client.get`.
- Ensure the mock API handles the `params` (dataInicial, dataFinal, pagina) correctly to simulate realistic pagination and date-range behavior.

## 4. Risks
- Mocking too much might miss real integration issues. Keep the mock as close to the API behavior as possible.
