# BDD Alignment Actions Log

This document records the decisions and actions taken by the Baliza BDD Alignment & Improvement Agent.

## 2024-07-24: CLI Alignment and Tier System Implementation

- **Action:** Refactored the CLI to use a `state` subcommand structure.
  - **Decision:** Moved the old `status` command to `baliza state show` and the `verify` command to `baliza state gaps` to better align with the project's documentation and BDD features.
  - **Reason:** This creates a more organized and scalable CLI interface that matches the user journeys described in the BDD scenarios.

- **Action:** Implemented the Tier system infrastructure.
  - **Decision:** Created `src/baliza/tiers.py` with `FeatureTier` enum and decorators (`@tier0`, etc.) and added the `baliza tiers` command.
  - **Reason:** To formalize the feature hierarchy and provide transparency to operators about the criticality and stability of different commands.

- **Action:** Implemented the `backfill` command.
  - **Decision:** Added `baliza backfill <YYYY-MM> <YYYY-MM>` to support historical data extraction as promised in the `README.md`.
  - **Reason:** To complete the core feature set for historical data management.

- **Action:** Updated `extractor.py` to record run history.
  - **Decision:** The `PNCPExtractor.extract` method now records its execution start and finish in the `baliza_state.runs` table.
  - **Reason:** This enables the `baliza state history` command and improves observability of the extraction pipeline.

- **Action:** Aligned BDD Tests and Documentation.
  - **Decision:** Updated `tests/step_defs/test_state_management.py` and `tests/features/verification.feature` to use the new CLI structure. Rewrote `README.md` and `feature-hierarchy.md` to be accurate and honest about the implementation.
  - **Reason:** To ensure the "living specification" (BDD) remains executable and the documentation reflects reality.

- **Outcome:** The codebase, BDD features, and documentation are now fully aligned. All non-quarantined BDD tests are passing.
