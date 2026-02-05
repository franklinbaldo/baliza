# MASTERPLAN: Baliza CLI

**This is the living strategic plan for the `baliza` CLI project.** It defines the project's goals, scope, and prioritized backlog. This document is maintained by the PM/Goal-Alignment Agent and serves as the authoritative source for development priorities.

## 1. North Star & Goals

### North Star

To be the most reliable, transparent, and accessible tool for extracting and preserving Brazilian public procurement data from the National Public Procurement Portal (PNCP), enabling accountability and research for journalists, civil society, and government agencies.

### Concrete Goals

1.  **Achieve Full Extraction Resumability:** Implement a robust state management system that makes the extraction process fully resumable and idempotent, recovering gracefully from network failures or API instability.
2.  **Comprehensive Endpoint Coverage:** Expand beyond the initial `contratos` endpoint to support all relevant PNCP data sources.
3.  **Automated Data Publishing:** Establish a fully automated CI/CD pipeline to extract data, export it to Parquet, and publish versioned datasets.
4.  **Actionable Data Quality Monitoring:** Develop tools to verify data coverage, detect gaps, and provide clear reports on the completeness and integrity of the extracted data.
5.  **Excellent Developer/Operator Experience:** Provide clear documentation and a simple, predictable command-line interface.

## 2. Non-Goals / Anti-Scope

-   **No Frontend/UI:** This repository is exclusively for the backend CLI data pipeline.
-   **No Ad-hoc Analysis Features:** The CLI's purpose is data extraction and preservation, not complex data analysis.
-   **No Real-time Data Streaming:** The pipeline is designed for batch processing.

## 3. Architecture Constraints

-   **Python & HTTPX:** Core pipeline built on Python using `httpx`.
-   **DuckDB for Staging:** DuckDB serves as the local "bronze" layer for raw data and state management.
-   **Parquet for Archival:** Apache Parquet is the official "gold" data format for archival and public consumption.
-   **Stateless by Default, Stateful via explicit State File:** The CLI uses `baliza.duckdb` for resumability and gap detection.

## 4. Current Status (July 2024)

### ✅ Implemented
- Basic extraction (`baliza extract`) with page-level checkpointing.
- Coverage tracking for processed date windows.
- Gap detection via `baliza verify`.
- Parquet export (`baliza export`, `baliza export-daily`).
- Buffer statistics (`baliza buffer-stats`).
- Simple status dashboard (`baliza status`).

### ⏳ In Progress / Missing
- **State Management CLI:** Consolidating `status`, `verify`, and `buffer-stats` into a unified `baliza state` command.
- **Backfill Command:** Automated month-by-month historical extraction.
- **Resilience Verification:** Proper BDD tests for transient failure recovery.
- **Multi-endpoint support:** Adding `compras`, `licitacoes`, etc.

## 5. Prioritized Backlog

### Epic 1: Unified State & Observability (High Priority)
- [ ] Implement `baliza state show` (merging `status` and `buffer-stats`).
- [ ] Implement `baliza state gaps` (merging into `verify` or as a subcommand).
- [ ] Implement `baliza state history` (tracking extraction runs in `baliza_state.runs`).

### Epic 2: Historical Consolidation
- [ ] Implement `baliza backfill YYYY-MM YYYY-MM` for easy historical extraction.

### Epic 3: Pipeline Integrity
- [ ] Implement resilience BDD tests with proper mocking.
- [ ] Add data validation checks (schema adherence, record counts).

### Epic 4: Expansion
- [ ] Support additional PNCP endpoints.

## 6. Test Strategy

-   **Unit Tests:** Pure functions and state management logic.
-   **Integration Tests:** Interaction between `httpx`, `StateManager`, and DuckDB.
-   **BDD Tests:** Living specification using Gherkin features.
-   **Quarantine Policy:** Flaky tests or those requiring external services are marked as skipped/quarantined with clear reasons.
