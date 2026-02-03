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
- **Status:** ✅ COMPLETED. README.md was updated to reflect the httpx architecture.

## 2026-02-11: PM Discovery - Documentation Drift and Fake Tests

- **Concern:** A significant drift has been discovered between the project's documentation (Masterplan, Feature Hierarchy, Roadmap) and the actual implementation in `cli_simple.py`. Several commands (`state show`, `state gaps`, `state history`, `backfill`, `tiers`) are documented as "Done" but are missing from the code. Furthermore, `test_state_management.py` uses mocks to simulate these commands, hiding the implementation gap.
- **Evidence:**
    - `src/baliza/cli_simple.py`: Missing documented commands.
    - `tests/step_defs/test_state_management.py`: Uses `runner.invoke(app, ["status", ...])` instead of `state show`, and mocks `state history` entirely.
    - `docs/alignment/feature-hierarchy.md`: Claims `baliza tiers` and other commands are implemented.
- **Actions Taken:**
    - Updated `feature_goal_matrix.md`, `feature-hierarchy.md`, and `MASTERPLAN.md` to accurately reflect the "Planned" status of missing features.
    - Translated `ROADMAP.md` to English and updated it to reflect current priorities.
    - Created feature briefs (002, 003, 004) to guide the implementation of the missing functionality.
    - Marked faked BDD tests as `xfail`.
- **Owner:** Jules (PM Agent)
- **Status:** Alignment synchronized. Implementation briefs handed over.
