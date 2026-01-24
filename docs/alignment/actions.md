# BDD Alignment Actions

This document records the actions taken to align the BDD test suite with the project's actual goals and implementation.

## Session 1: Initial Alignment

**Date:** 2024-07-15

**Objective:** Correct the severe misalignment between the outdated `README.md`, aspirational BDD tests, and the simplified, functional codebase.

### Actions Taken:

1.  **Code and Documentation Review:** Analyzed `README.md`, `pyproject.toml`, `src/baliza/cli_simple.py`, and `src/baliza/extractor.py`. Confirmed that the `dlt`-based, resumable pipeline described in the README does not exist. The current implementation is a simpler `httpx` + `duckdb` extractor.
2.  **Established Ground Truth:** Created the `docs/alignment/` directory to serve as the canonical source of truth for project goals, inventory, and BDD alignment.
    -   `inventory.md`: Documented the testing frameworks and how to run tests.
    -   `goals.md`: Defined the project's actual goals based on the current codebase.
    -   `feature_goal_matrix.md`: Assessed the alignment of each BDD feature.
3.  **BDD Feature Realignment Plan:**
    -   **Retire:** `resilience.feature` and `buffer_management.feature` will be deleted as they test non-existent functionality.
    -   **Rewrite:** `end_to_end_extraction.feature` will be rewritten to test the simple, date-range-based extraction.
    -   **Review and Simplify:** `verification.feature` will be reviewed and simplified to match the current verification command.
4.  **`README.md` Overhaul:** Planned a complete rewrite of the `README.md` to accurately reflect the project's current, simpler architecture and CLI commands.
