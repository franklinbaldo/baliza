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

## 2026-02-01: Implementation of Resilience and State History

- **Action:** Implemented BDD step definitions for \`resilience.feature\` and \`state_management.feature\`.
- **Reason:** These features were previously defined but not implemented, causing a gap between documentation and reality.
- **Details:**
    - Enhanced \`PNCPExtractor\` in \`src/baliza/extractor.py\` to record extraction runs in \`baliza_state.runs\`.
    - Added \`state\` command group to \`src/baliza/cli_simple.py\` with \`show\`, \`gaps\`, and \`history\` subcommands.
    - Created shared BDD fixtures in \`tests/step_defs/conftest.py\`.
    - Updated \`test_resilience.py\` to use internal retries and resumability checks.
    - Updated \`test_state_management.py\` to use real CLI commands instead of mocks.
- **Outcome:** Core reliability and observability features are now fully tested and functional.
