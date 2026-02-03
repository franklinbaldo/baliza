# BDD Alignment Actions Log

This document records the decisions and actions taken by the Baliza BDD Feature Builder Agent.

## 2024-07-24: PM Escalation Note - Outdated README.md

-   **Concern:** The root `README.md` is dangerously outdated and describes an architecture that is no longer in use. It references a `dlt`-based pipeline, a `src/baliza/config/pncp.yml` configuration file, and a `baliza.duckdb` file structure that do not match the current, simpler `httpx`-based implementation. This creates a significant risk of confusion for new developers and users.
-   **Evidence:**
    -   `README.md`: Describes a `dlt`-based pipeline.
    -   `src/baliza/extractor.py`: Shows a simpler `httpx`-based implementation.
    -   `src/baliza/cli_simple.py`: Shows the current CLI structure.
-   **Concrete Proposal:** The `README.md` needs a complete rewrite to accurately reflect the current architecture. This is a high-priority task that falls under the PM agent's purview. The BDD Feature Builder has established a correct baseline in the `docs/alignment/` directory, which can be used as a source of truth for the rewrite.
-   **Owner:** Baliza BDD Feature Builder
-   **Status:** Action required by PM agent.

## 2026-02-11: BDD Alignment and Test Stabilization Pass

-   **Action:** Performed a comprehensive alignment pass between documentation, BDD features, and CLI implementation.
-   **Changes:**
    -   **README.md:** Rewrote to match `cli_simple.py` capabilities, removing non-existent commands (`backfill`, `state`, `tiers`).
    -   **test_end_to_end_extraction.py:** Refactored to use `unittest.mock.patch` instead of `pytest-httpx`, resolving persistent timeout issues and removing from quarantine.
    -   **test_resilience.py:** Fully implemented step definitions for transient failure and retry-limit scenarios.
    -   **state_management.feature:** Aligned scenarios with actual CLI commands (`status`, `verify`).
    -   **test_state_management.py:** Updated to match new Gherkin and marked unimplemented `--history` as `xfail(strict=True)`.
    -   **feature-hierarchy.md:** Rewritten to accurately reflect the 4-tier system and implementation status.
    -   **feature_goal_matrix.md:** Updated to show actual progress (8 features, 21 scenarios).
    -   **quarantine.md:** Renewed and updated with current state.
    -   **daily_exporter.py:** Fixed `utcnow()` deprecation warning.
-   **Status:** Successfully stabilized core tests and aligned documentation with reality.
-   **Owner:** Baliza BDD Alignment Agent (Jules)
