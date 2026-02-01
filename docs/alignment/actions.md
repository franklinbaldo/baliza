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

## 2024-07-24: BDD Test Suite Stabilization and Command Implementation

-   **Action:** Resolved quarantine of `end_to_end_extraction.feature` by replacing `pytest-httpx` with `monkeypatch` in step definitions.
-   **Action:** Implemented missing BDD step definitions for `resilience.feature` and `state_management.feature`.
-   **Action:** Enhanced `src/baliza/extractor.py` and `src/baliza/cli_simple.py` to record extraction runs in `baliza_state.runs`.
-   **Action:** Implemented the `baliza state` command group (show, gaps, history) in `src/baliza/cli_simple.py`.
-   **Action:** Synchronized `docs/alignment/feature_goal_matrix.md` with the full set of 8 BDD features currently in the codebase.
-   **Owner:** Baliza BDD Feature Builder
-   **Status:** Completed.
