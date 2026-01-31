# Feature Brief: CLI Refactor (State Subcommands & Tier System)

-   **ID:** 002
-   **Epic:** CLI Refactor & Tier System
-   **Goal Mapping:** Excellent Developer/Operator Experience
-   **Status:** Ready for Implementation

## 1. User Story

As a developer and operator of the Baliza CLI, I want a unified and well-organized command-line interface that reflects the project's feature hierarchy, so that it is easier to discover functionality, monitor the system state, and maintain the codebase.

## 2. Acceptance Criteria (BDD-Style)

### Scenario 1: Tier System Infrastructure
- **Given** I am a developer
- **When** I look at the codebase
- **Then** I should find a `src/baliza/tiers.py` file containing a `FeatureTier` Enum and `@tier0`, `@tier1`, `@tier2`, `@tier3` decorators.
- **And** every command in `cli_simple.py` should be decorated with its appropriate tier.

### Scenario 2: Unified State Subcommands
- **Given** the Baliza CLI is installed
- **When** I run `baliza state show`
- **Then** it should execute the logic previously found in `baliza status`.
- **When** I run `baliza state gaps`
- **Then** it should execute the logic previously found in `baliza verify`.
- **When** I run `baliza state history`
- **Then** it should display a list of previous extraction runs from the `baliza_state.runs` table.

### Scenario 3: Tiers Discovery
- **Given** the Baliza CLI is installed
- **When** I run `baliza tiers`
- **Then** I should see a formatted list of all available commands organized by their Tier (🔴 🟠 🟡 ⚪).

### Scenario 4: Clean Test Mapping
- **Given** the `tests/step_defs/test_state_management.py` file
- **When** I run the BDD tests
- **Then** the step definitions should call the ACTUAL `baliza state` subcommands (no more internal mapping of `state show` to `status`).

## 3. Data Contracts

-   **Tiers Metadata:** The `FeatureTier` enum should have:
    - `TIER_0`: "Critical Path" (🔴)
    - `TIER_1`: "Core Features" (🟠)
    - `TIER_2`: "Operator Experience" (🟡)
    - `TIER_3`: "Future Enhancements" (⚪)
-   **State History Query:**
    ```sql
    SELECT run_id, started_at, finished_at, status, rows_extracted
    FROM baliza_state.runs
    ORDER BY started_at DESC
    LIMIT 10;
    ```

## 4. Observability

-   **CLI Output:** Use `rich` for all new command outputs to maintain visual consistency.
-   **Tier Badges:** The `tiers` command and help messages should display the tier emoji badges.

## 5. Out-of-Scope

-   Refactoring the `extractor.py` logic.
-   Implementing new extraction endpoints.
-   Fixing timeout issues in quarantined tests (this brief is focused on CLI structure).

## 6. Risk Notes

-   **Breaking Changes:** Renaming `status` to `state show` and `verify` to `state gaps` are breaking changes for existing scripts. We accept this now to reach a cleaner v1.0 interface.
-   **DuckDB Locking:** Ensure `state` commands open the database in read-only mode where possible to avoid locking out active extractions.

## 7. Checklist for Implementer

- [ ] Create `src/baliza/tiers.py`.
- [ ] Add `state` subcommand group to `src/baliza/cli_simple.py`.
- [ ] Move/Rename `status` -> `state show`.
- [ ] Move/Rename `verify` -> `state gaps`.
- [ ] Implement `state history`.
- [ ] Implement `baliza tiers`.
- [ ] Decorate all commands in `cli_simple.py`.
- [ ] Update `tests/step_defs/test_state_management.py` to use new command names.
- [ ] Remove `@pytest.mark.skip` from `test_show_history` in `tests/step_defs/test_state_management.py`.
- [ ] Verify with `uv run pytest tests/features/state_management.feature`.
