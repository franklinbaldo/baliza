# BDD Alignment Actions Log

This document records the decisions and actions taken by the Baliza BDD Alignment & Improvement Agent.

## 2026-02-12: CLI Alignment and Test Stabilization

- **Problem:** The CLI implementation in `src/baliza/cli_simple.py` did not match the structure described in `README.md`. BDD tests were using mocks or aliases. `test_resilience.py` and `test_end_to_end_extraction.py` were failing or skipped.
- **Action taken:**
    1. **CLI Refactoring:** Refactored `src/baliza/cli_simple.py` to introduce the `state` command group (`show`, `gaps`, `history`) and added a skeleton `backfill` command.
    2. **Run History Implementation:** Updated `PNCPExtractor.extract` in `src/baliza/extractor.py` to record execution history in `baliza_state.runs`.
    3. **Test Stabilization:** Fixed `tests/step_defs/test_end_to_end_extraction.py` by replacing `pytest-httpx` with `monkeypatch`, resolving persistent timeout issues.
    4. **BDD Implementation:** Fully implemented `tests/step_defs/test_resilience.py` and aligned `tests/step_defs/test_state_management.py` with the new CLI.
    5. **Documentation Alignment:** Updated `inventory.md`, `feature_goal_matrix.md`, `quarantine.md`, and verified `README.md`.
- **Status:** COMPLETED. All core BDD features are now passing and aligned with the product.

## 2024-07-24: PM Escalation Note - Outdated README.md (RESOLVED)

- **Concern:** README.md was referencing `dlt`.
- **Status:** Resolved in subsequent updates.
