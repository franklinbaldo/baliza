# MASTERPLAN: Baliza CLI

**This is the living strategic plan for the `baliza` CLI project.** It defines the project's goals, scope, and prioritized backlog. This document is maintained by the PM/Goal-Alignment Agent and serves as the authoritative source for development priorities.

## 1. North Star & Goals

### North Star

To be the most reliable, transparent, and accessible tool for extracting and preserving Brazilian public procurement data from the National Public Procurement Portal (PNCP), enabling accountability and research for journalists, civil society, and government agencies.

### Concrete Goals

1.  **Achieve Reliable, Resumable Extraction:** The core `PNCPExtractor` must gracefully handle network failures and API instability through its page-level checkpointing system, ensuring that long-running extractions can always be resumed without data loss.
2.  **Establish a Long-Term Public Archive:** Formalize the workflow of exporting daily data snapshots to Parquet and publishing them to a stable, long-term repository like the Internet Archive, ensuring permanent public access to the data.
3.  **Provide Simple, Verifiable Data Coverage:** Allow operators to easily verify the completeness of the extracted data and identify any gaps in coverage with simple, clear CLI commands.
4.  **Excellent Developer/Operator Experience:** Provide clear documentation, a straightforward command-line interface, and predictable behavior to make `baliza` easy to use, automate, and maintain.

## 2. Non-Goals / Anti-Scope

-   **No Frontend/UI:** This repository is exclusively for the backend CLI data pipeline. All visualization and web interfaces belong in the separate `baliza-site` repository.
-   **No Ad-hoc Analysis Features:** The CLI's purpose is data extraction and preservation. Users should consume the exported Parquet/DuckDB files with their own tools (e.g., pandas, Polars).
-   **No Real-time Data Streaming:** The pipeline is designed for robust, daily batch processing.
-   **No Non-PNCP Data Sources:** The scope is strictly limited to data provided by the official PNCP API.

## 3. Architecture

-   **Core Logic:** A custom Python extractor (`PNCPExtractor`) uses `httpx` to make direct calls to the PNCP API. It is designed to be simple, robust, and maintainable.
-   **State Management & Local Buffer:** A local DuckDB file (`baliza.duckdb`) serves two purposes: it acts as a temporary buffer for raw JSON data, and it holds all state-management tables (`baliza_state` schema) for checkpointing, coverage tracking, and archival status.
-   **Archival Format:** Apache Parquet is the official "gold" data format for long-term archival, created by the `export-daily` command.
-   **Archival Target:** The primary destination for the exported Parquet artifacts is the Internet Archive, ensuring a permanent, public, and immutable record of the data.

## 4. Prioritized Backlog

### Epic 1: Stabilize the Core Pipeline & Archive Workflow

*   **Feature:** Add comprehensive integration tests for `PNCPExtractor`, covering success, failure, and resume scenarios.
*   **Feature:** Formalize the Internet Archive upload process, potentially with a new CLI command or a well-documented helper script.
*   **Feature:** Improve the `verify` command to provide more detailed and user-friendly gap analysis reports.

### Epic 2: Expand Endpoint Coverage

*   **Feature:** Add support for the `compras` (procurements) endpoint using the existing `PNCPExtractor` pattern.
*   **Feature:** Refactor `PNCPExtractor` to gracefully handle different data schemas and primary keys from new endpoints.

### Epic 3: Improve Observability and Operations

*   **Feature:** Implement structured logging (e.g., JSON format) to make the CLI's output more easily machine-readable for automation.
*   **Feature:** Enhance the `status` command to provide a more detailed overview of the local buffer, archival status, and data coverage.

## 5. Test Strategy

-   **Unit Tests:** Focus on pure functions in `utils.py`, such as date formatting and identifier validation.
-   **Integration Tests:** Test the `PNCPExtractor`'s interaction with a mocked `httpx` client and its state management logic against a temporary DuckDB database.
-   **End-to-End (E2E) Tests:** Full CLI runs (`extract`, `export-daily`, `verify`, `status`) using pre-recorded API responses and a fixture-based DuckDB file to validate the entire workflow, from data extraction to Parquet output.

## 6. Known Gaps / Technical Debt

-   **Undocumented Internet Archive Workflow:** The process for uploading the daily Parquet exports to the Internet Archive is neither documented nor automated within the tool itself. This is a critical gap in the primary archival goal.
-   **Limited Endpoint Support:** The tool currently only supports the `contratos` endpoint.
-   **Basic `verify` Command:** The current gap detection logic is simplistic and could be made more robust.
-   **Outdated README:** The main `README.md` is severely out of sync with the application's actual functionality (this is being addressed).
