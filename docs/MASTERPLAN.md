# MASTERPLAN: Baliza CLI

**This is the living strategic plan for the `baliza` CLI project.** It defines the project's goals, scope, and prioritized backlog.

## 1. North Star & Goals

### North Star

To be the most reliable, transparent, and accessible tool for extracting and preserving Brazilian public procurement data from the National Public Procurement Portal (PNCP), enabling accountability and research for journalists, civil society, and government agencies.

### Concrete Goals

1.  **Achieve Full Extraction Resumability:** Implement a robust state management system that makes the extraction process fully resumable and idempotent, recovering gracefully from failures.
2.  **Autonomous Extraction:** Enable `baliza extract` to run without manual date range parameters, identifying and filling gaps automatically.
3.  **Actionable Observability:** Provide a clear `state` command to inspect coverage, gaps, and execution history.
4.  **Excellent Developer/Operator Experience:** Maintain high-quality BDD features and documentation that match the implementation.

## 2. Non-Goals / Anti-Scope

-   **No Frontend/UI:** Handled in `baliza-site`.
-   **No Ad-hoc Analysis Features:** Users should consume the exported Parquet/DuckDB files.
-   **No Real-time Data Streaming:** Designed for batch processing.

## 3. Architecture Constraints

-   **Python, HTTPX, & DuckDB:** Direct, resilient HTTP requests with local DuckDB staging.
-   **Parquet for Archival:** Partitioned by year/month.
-   **State in DuckDB:** All state management (coverage, runs, checkpoints) resides in the `baliza_state` schema within the DuckDB file.

## 4. Prioritized Backlog

### Phase 1: State Management & CLI Alignment (CURRENT)
- [ ] **Feature:** Implement `state` subcommand group (`show`, `gaps`, `history`).
- [ ] **Feature:** Properly record every extraction run in `baliza_state.runs`.
- [ ] **Feature:** Refactor `extract` and `verify` logic to share the same state-reading foundations.
- [ ] **Cleanup:** Align BDD tests to use the real `state` commands.

### Phase 2: Autonomous Extraction
- [ ] **Feature:** Add `--autonomous` mode to `extract` (default when no dates provided).
- [ ] **Feature:** Implement intelligent lookback logic.
- [ ] **Feature:** Automated gap-filling in the extraction loop.

### Phase 3: Data Integrity & Expansion
- [ ] **Feature:** Page-level hash verification to detect data drift.
- [ ] **Feature:** Add support for `compras` and `licitacoes` endpoints.
- [ ] **Feature:** Refactor config to support multiple resources easily.

## 5. Test Strategy

-   **Unit & Integration Tests:** Use VCR cassettes for API stability.
-   **BDD Features:** Primary source of truth for CLI behavior.
-   **CI/CD:** Enforce linting (ruff) and tests (pytest) on all PRs.

## 6. Known Gaps / Technical Debt

-   **Test Timeouts:** `pytest-httpx` is currently causing timeouts in some environments, leading to test quarantine.
-   **Manual Date Entry:** `extract` still requires explicit `--start` and `--end` for most operations.
-   **Implicit State:** Some state logic is duplicated between `extractor.py` and `cli_simple.py`.
