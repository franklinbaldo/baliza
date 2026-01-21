# MASTERPLAN

This document is the single source of truth for the Baliza project's goals,
architecture, and prioritized backlog. It is maintained by the PM/Goal-Alignment
Agent.

## 1. North Star

**To be the most reliable, comprehensive, and accessible source of Brazilian public procurement data for journalists, researchers, and oversight bodies.**

We achieve this by building a resilient, transparent, and easy-to-operate data extraction pipeline that preserves the complete history of the PNCP (Portal Nacional de Contratações Públicas).

## 2. Goals

1.  **Preserve Public History:** Capture and archive every version of every public
    contract from the PNCP, ensuring a permanent, auditable record.
2.  **Provide a Robust Pipeline:** Deliver a stateful, resumable, and observable
    extraction CLI that is easy to deploy and operate in production environments.
3.  **Enable Analysis:** Make the data immediately accessible for analysis through
    a well-structured DuckDB database and partitioned Parquet exports.

## 3. Non-Goals / Anti-Scope

-   **Data Visualization and Web UI:** This is the responsibility of the separate
    `baliza-site` project. The Baliza CLI focuses exclusively on the data
    pipeline.
-   **Real-time Data Streaming:** The current architecture is batch-oriented,
    focused on daily windows. While low-latency updates are a potential future
    goal, real-time is not a current priority.
-   **Complex Data Transformations:** The CLI is responsible for extraction and
    light transformation (schema alignment). In-depth analysis, scoring, or
    joining with external datasets is out of scope for this tool.

## 4. Architecture

The Baliza CLI is a Python application built with Typer, httpx, and DuckDB. The
previous architecture based on the `dlt` library has been fully **removed** in
favor of a more direct, stateful implementation.

-   **Core Logic:** The `PNCPExtractor` class in `src/baliza/extractor.py`
    manages all interaction with the PNCP API. It is responsible for fetching
    data in paginated windows, handling responses, and loading data into DuckDB.
-   **State Management:** The application's state is stored in a local DuckDB
    file (`baliza.duckdb` by default). A dedicated schema, `baliza_state`,
    contains two key tables:
    -   `coverage`: Tracks the status (`complete`, `failed`) of each daily
        extraction window for each resource. This is the foundation of the
        resumable pipeline.
    -   `runs`: Logs each execution of the `extract` command, providing an
        auditable history of pipeline activity.
-   **Data Storage:** Raw data extracted from the PNCP is stored in tables within
    a user-defined schema (default: `baliza_raw`). Data is inserted using an
    "INSERT OR IGNORE" strategy, using the `numeroControlePNCP` as the primary
    key to ensure idempotency.
-   **Extensibility:** The `PNCPExtractor` is designed to be extended to support
    new PNCP resources (e.g., `compras`, `atas`) by adding new table schemas and
    API handling logic.

## 5. Prioritized Backlog

### Epic 1: Improve Extensibility

-   **Feature: Multi-Resource Extraction:** Refactor `PNCPExtractor` to handle
    different resources (`contratos`, `compras`, etc.) without hardcoding table
    names and schemas.
    -   **Scenario:** A user can run `baliza extract --resource compras` to fetch
        data from the `/v1/compras` endpoint into a `compras` table.
-   **Feature: Configuration-driven Schemas:** Move DuckDB table schemas from
    hardcoded strings into a configuration file or class structure to simplify
    adding new resources.

### Epic 2: Enhance State Management & Verification

-   **Feature: Comprehensive `verify` command:** Improve the `baliza verify`
    command to perform more robust checks, such as validating row counts against
    API metadata and detecting hash mismatches.
-   **Feature: Intelligent Window Merging:** Optimize the extractor to merge
    small, contiguous unprocessed windows into a single API call to reduce
    overhead.

## 6. Known Gaps / Technical Debt

-   **Outdated Documentation (High Priority):** The `README.md` is critically
    out of sync with the codebase, referencing the old `dlt`-based architecture.
    This must be updated immediately.
-   **Hardcoded Configuration:** The schema for the `contratos` resource is
    hardcoded within `PNCPExtractor`. This makes it difficult to add support
    for new resources.
-   **Limited Testing:** The current test suite is minimal. Coverage needs to be
    expanded, particularly for the state management and extraction logic.
