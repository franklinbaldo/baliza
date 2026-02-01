# MASTERPLAN: Baliza CLI

**This is the living strategic plan for the `baliza` CLI project.** It defines the project's goals, scope, and prioritized backlog.

## 1. North Star & Goals

### North Star

To be the most reliable, transparent, and accessible tool for extracting and preserving Brazilian public procurement data from the National Public Procurement Portal (PNCP), enabling accountability and research.

### Concrete Goals

1.  **Achieve Full Extraction Resumability:** Robust state management with per-page checkpoints. (✅ Core logic implemented)
2.  **Comprehensive Endpoint Coverage:** Support all relevant PNCP data sources. (⏳ In progress: `contratos` only)
3.  **Automated Data Publishing:** Establish a CI/CD pipeline for data releases. (⏳ Planned in `baliza-site`)
4.  **Actionable Data Quality Monitoring:** Tools to verify coverage and detect gaps. (✅ `verify` implemented, ⏳ `state` commands in progress)
5.  **Excellent Developer/Operator Experience:** Clear CLI and documentation. (✅ Refined architecture)

## 2. Non-Goals / Anti-Scope

-   **No Frontend/UI:** Belongs in `baliza-site`.
-   **No Ad-hoc Analysis Features:** Users should use DuckDB/Parquet directly.
-   **No Real-time Data Streaming:** Designed for batch processing.

## 3. Architecture Constraints

-   **Python & HTTPX:** Core pipeline uses `httpx` for resilience.
-   **DuckDB for Staging:** Bronze layer for raw data and state management.
-   **Parquet for Publishing:** Official gold data format.
-   **No dlt:** The project uses a custom, lightweight extraction logic instead of the `dlt` library for better control over PNCP-specific pagination and error handling.

## 4. Prioritized Backlog

### Epic 1: Core CLI Expansion (Current Focus)

*   **Feature:** Implement `state` command group (`show`, `gaps`, `history`) for observability.
*   **Feature:** Implement `backfill` command for month-by-month processing.
*   **Feature:** Refactor BDD tests to use these new commands.

### Epic 2: Data Quality & Verification

*   **Feature:** Enhance the `verify` command with anomaly detection.
*   **Feature:** Support for detecting "late arrivals" in PNCP data.

### Epic 3: Expanded Endpoint Coverage

*   **Feature:** Add support for the `compras` endpoint.
*   **Feature:** Add support for the `licitacoes` endpoint.

## 5. Test Strategy

-   **BDD-First:** Features are defined in Gherkin and tested with `pytest-bdd`.
-   **Unit Tests:** For utilities and pure logic.
-   **Integration Tests:** Against mock PNCP API.
-   **Tiered Testing:** Tests are marked with `tier0` to `tier3` to prioritize critical paths.

## 6. Known Gaps / Technical Debt

-   **Documentation Drift:** Some docs still refer to the old `dlt` architecture. (Partially addressed)
-   **Test Workarounds:** `state_management.feature` uses mocked/mapped commands because the CLI hasn't implemented them yet. (Addressed in Epic 1)
