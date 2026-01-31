# MASTERPLAN: Baliza CLI

**This is the living strategic plan for the `baliza` CLI project.** It defines the project's goals, scope, and prioritized backlog.

## 1. North Star & Goals

### North Star

To be the most reliable, transparent, and accessible tool for extracting and preserving Brazilian public procurement data from the National Public Procurement Portal (PNCP), enabling accountability and research for journalists, civil society, and government agencies.

### Concrete Goals

1.  **Reliable Data Extraction & Resumability:** Maintain a robust pipeline that recovers gracefully from network failures using a per-page checkpoint system.
2.  **Daily Export Packages:** Provide self-contained, relational Parquet packages (contratos, orgaos, unidades) suitable for long-term preservation on services like Internet Archive.
3.  **Actionable Data Quality Monitoring:** Detect gaps and inconsistencies in coverage through a dedicated verification system.
4.  **Comprehensive Endpoint Coverage:** Expand beyond the `contratos` endpoint to support all relevant PNCP data sources.
5.  **Excellent Developer/Operator Experience:** Provide a simple, predictable CLI and clear documentation.

## 2. Non-Goals / Anti-Scope

-   **No Frontend/UI:** All visualization and dashboards belong in the separate `baliza-site` repository.
-   **No Ad-hoc Analysis Features:** Users should consume the exported Parquet/DuckDB files with their own tools.
-   **No Real-time Data Streaming:** The pipeline is designed for batch processing.

## 3. Architecture Constraints

-   **Python & HTTPX:** Core pipeline built with `httpx` for resilient API calls.
-   **DuckDB:** Local staging and state management.
-   **Parquet:** Official gold format for public consumption.
-   **IA-First Export:** Optimization for self-contained daily snapshots intended for Internet Archive.

## 4. Prioritized Backlog

### Epic 1: CLI Refactor & Tier System (Current Priority)

*   **Feature:** Refactor CLI to use `state` subcommands (`state show`, `state gaps`, `state history`) to unify `status` and `verify`.
*   **Feature:** Implement the Feature Tier system (@tier0, @tier1, etc.) in code to guide maintenance and testing.
*   **Feature:** Fix quarantined BDD tests to ensure a reliable "Green" baseline.

### Epic 2: Expanded Endpoint Coverage

*   **Feature:** Add support for the `compras` (procurements) endpoint.
*   **Feature:** Add support for the `licitacoes` (tenders) endpoint.
*   **Feature:** Refactor the configuration to easily support multiple endpoints.

### Epic 3: Advanced Data Quality

*   **Feature:** Enhance `verify` (or `state gaps`) to detect late updates in the API.
*   **Feature:** Add record-level hashing to identify silent data corruption.

## 5. Test Strategy

-   **BDD (Pytest-BDD):** Primary way to define and test business logic.
-   **Integration:** Interaction between `httpx` and DuckDB using VCR cassettes.
-   **Quarantine Policy:** Tests that are flaky due to external factors are kept in `docs/alignment/quarantine.md` until fixed.

## 6. Known Gaps / Technical Debt

-   **CLI Command Inconsistency:** Current CLI commands (`status`, `verify`) are disjoint; needs unification under `state` group.
-   **Tier System Mismatch:** Documentation describes a Tier system that is not yet implemented in the codebase.
-   **Test Timeouts:** Some integration tests are currently skipped due to environment-related timeouts.
