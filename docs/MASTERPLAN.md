# MASTERPLAN: Baliza CLI

**This is the living strategic plan for the `baliza` CLI project.** It defines the project's goals, scope, and prioritized backlog. This document is maintained by the PM/Goal-Alignment Agent and serves as the authoritative source for development priorities.

## 1. North Star & Goals

### North Star

To be the most reliable, transparent, and accessible tool for extracting and preserving Brazilian public procurement data from the National Public Procurement Portal (PNCP), enabling accountability and research for journalists, civil society, and government agencies.

### Concrete Goals

1.  **Achieve Full Extraction Resumability:** Implement a robust state management system that makes the extraction process fully resumable and idempotent, recovering gracefully from network failures or API instability.
2.  **Comprehensive Endpoint Coverage:** Expand beyond the initial `contratos` endpoint to support all relevant PNCP data sources, providing a complete picture of the procurement lifecycle.
3.  **Automated Data Publishing:** Establish a fully automated CI/CD pipeline to extract data, export it to Parquet, and publish versioned, immutable datasets via GitHub Releases.
4.  **Actionable Data Quality Monitoring:** Develop tools to verify data coverage, detect gaps, and provide clear reports on the completeness and integrity of the extracted data.
5.  **Excellent Developer/Operator Experience:** Provide clear documentation, straightforward configuration, and a simple, predictable command-line interface.

## 2. Non-Goals / Anti-Scope

-   **No Frontend/UI:** This repository is exclusively for the backend CLI data pipeline. All visualization, web interfaces, and user-facing dashboards belong in the separate `baliza-site` repository.
-   **No Ad-hoc Analysis Features:** The CLI's purpose is data extraction and preservation, not complex data analysis or ad-hoc querying. Users should consume the exported Parquet/DuckDB files with their own tools (pandas, Polars, BI tools, etc.).
-   **No Real-time Data Streaming:** The pipeline is designed for batch processing (daily/monthly runs), not real-time data streaming from the PNCP.
-   **No Non-PNCP Data Sources:** The scope is strictly limited to data provided by the official PNCP API.

## 3. Architecture Constraints

-   **Python & dlt:** The core pipeline is built on Python, using the `dlt` (data load tool) library for declarative data extraction.
-   **DuckDB for Staging:** DuckDB serves as the local, "bronze" layer for raw data and state management.
-   **Parquet for Publishing:** Apache Parquet is the official "gold" data format for archival and public consumption, partitioned by year and month.
-   **GitHub Releases as Data Warehouse:** The canonical public data artifacts will be published as assets attached to versioned GitHub Releases, ensuring immutability and public access.
-   **Stateless by Default, Stateful via explicit State File:** The CLI should be able to run in a stateless mode, but gain its resumability and gap-detection capabilities from an explicit state file (`baliza.duckdb`).

## 4. Prioritized Backlog

### Epic 1: Resumable Extraction Pipeline (✅ Completed)

*   **Feature:** Implement `StateManager` for persistent run tracking.
*   **Feature:** Implement `GapDetector` to identify missing or incomplete data windows.
*   **Feature:** Integrate StateManager and GapDetector into the `extract` command.
*   **Feature:** Add `state` CLI commands (`show`, `gaps`, `history`) for observability.

### Epic 2: Automated Data Publishing

*   **Feature:** Create a GitHub Actions workflow for daily incremental extraction.
*   **Feature:** Enhance the workflow to export new data to Parquet.
*   **Feature:** Add a step to create a versioned GitHub Release and upload Parquet files as assets.
*   **Feature:** Implement a manifest file that lists all Parquet files in the release.

### Epic 3: Expanded Endpoint Coverage

*   **Feature:** Add support for the `compras` (procurements) endpoint.
*   **Feature:** Add support for the `licitacoes` (tenders) endpoint.
*   **Feature:** Refactor the configuration to easily support multiple endpoints.

### Epic 4: Data Quality & Verification

*   **Feature:** Enhance the `verify` command to use the new state management system.
*   **Feature:** Add anomaly detection for suspicious page counts or record numbers.
*   **Feature:** Generate and publish a public data coverage report.

## 5. Test Strategy

-   **Unit Tests:** Focus on pure functions in `utils`, `state` management logic, and CLI argument parsing. Mock external dependencies like the PNCP API.
-   **Integration Tests:** Test the interaction between the `dlt` pipeline, the `StateManager`, and the DuckDB database. Use VCR cassettes (or similar) to record and replay real API responses.
-   **End-to-End (E2E) Tests:** Full CLI runs (`extract`, `backfill`, `export`, `verify`) against a small, controlled set of recorded API responses. These tests should validate the final Parquet output and state file.
-   **CI:** All tests (unit, integration, E2E) must pass in a GitHub Actions workflow on every push and pull request to `main`.

## 6. Known Gaps / Technical Debt

-   **Limited Test Coverage:** The current test suite primarily covers happy paths and needs to be expanded with more unit tests and failure-case scenarios.
-   **Lack of Observability:** The CLI provides minimal structured output (logs, metrics). This will be improved as part of the state management implementation.
-   **Manual Publishing:** Data releases are currently a manual process. (This is addressed in Epic 2).
