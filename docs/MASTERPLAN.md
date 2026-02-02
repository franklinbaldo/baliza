# MASTERPLAN: Baliza CLI

**This is the living strategic plan for the `baliza` CLI project.** It defines the project's goals, scope, and prioritized backlog. This document is maintained by the PM/Goal-Alignment Agent and serves as the authoritative source for development priorities.

## 1. North Star & Goals

### North Star

To be the most reliable, transparent, and accessible tool for extracting and preserving Brazilian public procurement data from the National Public Procurement Portal (PNCP), enabling accountability and research for journalists, civil society, and government agencies.

### Concrete Goals

1.  **Achieve Full Extraction Resumability:** Maintain a robust state management system that makes the extraction process fully resumable and idempotent, recovering gracefully from network failures or API instability.
2.  **Comprehensive Endpoint Coverage:** Expand beyond the initial `contratos` endpoint to support all relevant PNCP data sources.
3.  **Automated Data Publishing:** Support automated pipelines to extract data, export it to Parquet, and provide metadata for publishing.
4.  **Actionable Data Quality Monitoring:** Provide tools to verify data coverage, detect gaps, and report on data integrity.
5.  **Excellent Developer/Operator Experience:** Provide clear documentation, straightforward configuration, and a simple, predictable CLI.

## 2. Non-Goals / Anti-Scope

-   **No Frontend/UI:** This repository is exclusively for the backend CLI data pipeline.
-   **No Ad-hoc Analysis Features:** The CLI's purpose is data extraction and preservation, not analysis.
-   **No Real-time Data Streaming:** The pipeline is designed for batch processing.

## 3. Architecture Constraints

-   **Python & HTTPX:** Core pipeline uses `httpx` for direct, resilient HTTP requests.
-   **DuckDB for Staging:** DuckDB serves as the "bronze" layer for raw data and state management.
-   **Parquet for Archival:** Apache Parquet is the official "gold" data format for public consumption.
-   **Stateless by Default, Stateful via State File:** The CLI gains resumability and gap-detection from the state file (`baliza.duckdb`).

## 4. Prioritized Backlog

### Epic 1: CLI Maturity & Observability
*   **Feature:** Implement `state` CLI commands (`show`, `gaps`, `history`) for observability.
*   **Feature:** Implement `backfill` command for historical extraction.
*   **Feature:** Stabilize BDD test suite (Resilience and E2E).

### Epic 2: Expanded Endpoint Coverage
*   **Feature:** Add support for `compras` and `licitacoes` endpoints.
*   **Feature:** Refactor configuration to support multiple resources easily.

### Epic 3: Data Quality & Verification
*   **Feature:** Enhance `verify` command with anomaly detection.
*   **Feature:** Implement data integrity checks (hashes, unique keys).

## 5. Test Strategy

-   **Unit Tests:** Focus on utilities and state logic.
-   **BDD Features:** Use `pytest-bdd` to document and verify all CLI behaviors.
-   **Resilience Testing:** Use stateful mocks (via `monkeypatch`) to simulate API failures and retries.
-   **Integration Tests:** End-to-end CLI runs against recorded or mocked API responses.

## 6. Known Gaps / Technical Debt

-   **Incomplete CLI:** Several planned commands are not yet implemented in the CLI layer.
-   **Quarantined Tests:** Core resumability tests are currently disabled due to environmental issues.
-   **Manual Multi-Endpoint Support:** Supporting new endpoints currently requires manual code additions.
