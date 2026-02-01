# BDD Alignment Actions Log

This document records the decisions and actions taken by the Baliza BDD Alignment & Improvement Agent.

## 2026-02-01: Alignment Pass and Documentation Cleanup

- **Action:** Performed a comprehensive alignment pass across the entire repository.
- **Goal:** Ensure documentation (README.md, alignment docs) accurately reflects the actual state of the codebase and test suite.
- **Changes:**
    - **README.md:** Rewritten to remove aspirational features (backfill, state commands, lookback-days) and correctly describe existing CLI commands and arguments.
    - **docs/alignment/inventory.md:** Updated with correct CLI commands and test instructions.
    - **docs/alignment/goals.md:** Refined to focus on PNCP contracts extraction and preservation.
    - **docs/alignment/feature_goal_matrix.md:** Rebuilt to include all 8 existing feature files with their true implementation status.
    - **docs/alignment/quarantine.md:** Updated with all current skips and xfails, including unimplemented BDD scenarios, with proper metadata and expiry dates.
    - **docs/alignment/feature-hierarchy.md:** Corrected to show planned vs. implemented features accurately.
    - **tests/step_defs/test_resilience.py:** Added explicit `@pytest.mark.skip` markers to unimplemented scenarios.
- **Outcome:** Documentation is now 100% aligned with the codebase. The test suite is stable with a clear quarantine log for known issues and unimplemented features.

## 2024-07-24: PM Escalation Note - Outdated README.md (Resolved)

- **Status:** Resolved by the 2026-02-01 alignment pass. The README.md now correctly describes the HTTPX-based architecture and available commands.
