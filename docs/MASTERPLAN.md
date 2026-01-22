# MASTERPLAN

This document outlines the strategic vision, goals, and technical direction for the
Baliza project. It is a living document, intended to be updated as the project
evolves.

## North Star

To be the most reliable and simple tool for extracting public procurement data
from Brazil's Portal Nacional de Contratações Públicas (PNCP), enabling
transparency and accountability.

## Goals

1.  **Reliable Data Extraction:** Provide a simple, robust, and correct CLI for
    extracting data from the PNCP API. The tool should be easy to run and its
    behavior easy to understand.
2.  **Data Preservation:** Store extracted data in a raw, immutable format suitable
    for long-term analysis. We prioritize preserving the original data structure
    as closely as possible.
3.  **Data Accessibility:** Enable users to easily query the extracted data
    locally and export it to common analytical formats like Parquet.
4.  **Excellent Developer/Operator Experience:** Maintain clear documentation, a
    healthy and meaningful test suite, and a straightforward contribution process.

## Non-Goals

-   **Data Visualization:** This project is a data extraction tool. A web
    interface, dashboards, or other visualizations are the responsibility of the
    separate `baliza-site` project.
-   **Complex Data Transformations:** The CLI is not an ETL tool. It should
    perform minimal transformations, focusing on getting data from the API into
    storage.
-   **Real-time Data Streaming:** The project is designed for batch extraction,
    not real-time data feeds.

## Architecture Constraints

-   **CLI First:** The primary interface is and will remain a command-line tool.
-   **DuckDB for Local Storage:** DuckDB is the primary storage engine for its
    simplicity, performance, and ease of use.
-   **Parquet for Export:** Parquet is the designated format for analytical exports due
    to its efficiency and wide adoption.
-   **Simple Tech Stack:** The core stack is Python, Typer (for the CLI), and
    `httpx` (for API communication). We avoid adding heavy frameworks unless
    absolutely necessary.
-   **Behavior-Driven Development (BDD):** The primary testing strategy for the
    CLI's behavior is BDD using `pytest-bdd`. This ensures our tests are aligned
    with user-facing features.

## Backlog

### Epic: Foundational Stability & Alignment

This epic focuses on ensuring the project has a solid, reliable foundation.

-   **Feature: Accurate Documentation**
    -   [x] Update `README.md` to match the current simplified implementation.
    -   [x] Create `MASTERPLAN.md` to document project goals and direction.

-   **Feature: Robust BDD Test Suite**
    -   **User Story:** As a developer, I want a comprehensive BDD test suite so
        that I can make changes with confidence.
    -   **Scenarios:**
        -   Add BDD tests for the `extract` command (happy path, error handling).
        -   Add BDD tests for the `verify` command (detecting gaps, no gaps).
        -   Add BDD tests for the `export` command.

### Epic: Endpoint Expansion

This epic focuses on increasing the breadth of data the tool can extract.

-   **Feature: Extract `contratacoes` (Procurements)**
    -   **User Story:** As a user, I want to extract procurement data
        (`contratacoes`) in addition to contracts so that I can analyze the
        entire procurement lifecycle.
    -   **Scenarios:**
        -   Implement extraction for the `contratacoes` endpoint.
        -   Add a corresponding schema in the `extractor.py`.
        -   Add BDD tests for `contratacoes` extraction.

## Known Gaps / Technical Debt

-   **Test Suite:** The current test suite is minimal and lacks coverage for the
    simplified `httpx`-based architecture. A major priority is to build out BDD
    tests.
-   **CI/CD:** The project currently lacks automated testing and release workflows.
    Setting up a CI/CD pipeline (e.g., with GitHub Actions) is a future priority.
