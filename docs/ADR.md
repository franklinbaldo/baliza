# Architectural Decision Records (ADRs) for the BALIZA Project

ADRs are short documents that capture a significant architectural decision, along with its context and consequences. They serve as a historical record of why the system is designed the way it is.

---

## **ADR-001: Adopt DuckDB as the Primary Database**

**Status:** Accepted

**Context:**
The BALIZA project requires a storage system for raw extracted data (JSON responses) and for transformed data. The requirements are:
1.  **High performance** for analytical (OLAP) queries.
2.  **Ease of setup and maintenance:** The system must be simple to run locally and in CI/CD environments without a dedicated database server.
3.  **Portability:** The entire database should be contained in a single file, facilitating sharing, backup, and archival.
4.  **Native support for modern formats:** It must efficiently read and write formats like Parquet and JSON.
5.  **Excellent Python integration:** It must have a robust API for integration with Pandas and the PyData ecosystem.

**Decision:**
We will adopt **DuckDB** as the sole database engine for the project. It will be used to:
1.  Store raw JSON responses in the `psa.pncp_raw_responses` table.
2.  Serve as the engine for dbt transformations, where the Bronze, Silver, and Gold layers will be materialized as tables and views.
3.  Be the final distribution format for the dataset on the Internet Archive.

**Consequences:**
*   **Positive:**
    *   **Zero-config:** No need to install, configure, or manage a database server. `pip install duckdb` is sufficient.
    *   **Analytical Performance:** As a columnar, vectorized database, DuckDB offers exceptional performance for the aggregations and complex queries typical of the Gold layer.
    *   **Full Portability:** The `baliza.duckdb` database is a single file that can be easily copied, versioned, and uploaded to the Internet Archive.
    *   **Ecosystem:** Native and efficient integration with Parquet, JSON, Pandas, and dbt covers all our ETL needs.
    *   **Space Efficiency:** Native ZSTD compression for text columns significantly reduces the on-disk size of the database.
*   **Negative:**
    *   **Single-Writer:** DuckDB is optimized for a single writer process, which requires a careful architecture (like the `PNCPWriter`) to centralize write operations and prevent contention.
    *   **Not an OLTP:** It is not suitable for high-concurrency transactional workloads, but this is outside BALIZA's scope.

---

## **ADR-002: Resilient and Task-Driven Extraction Architecture**

**Status:** Accepted

**Context:**
Extracting data from the PNCP is a long-running process prone to failures (network errors, API instability, rate limiting). A simple script that iterates through dates and pages is fragile: any interruption would result in losing all progress and require a full restart. We need a system that is:
1.  **Resilient:** Able to survive interruptions and resume from where it left off.
2.  **Idempotent:** Repeated executions should not cause data duplication or unnecessary work.
3.  **Monitorable:** Extraction progress must be clear and easy to track.
4.  **Efficient:** Must use concurrency to maximize download speed.

**Decision:**
We will implement a task-driven extraction architecture managed by a control table (`psa.pncp_extraction_tasks`) in DuckDB. The process will be divided into four distinct phases:
1.  **Planning:** Generates all tasks (a combination of endpoint, date, and modality) and inserts them into the control table with `PENDING` status.
2.  **Discovery:** For each pending task, fetches the first page to get metadata (e.g., `total_pages`) and updates the task status to `FETCHING`.
3.  **Execution:** Concurrently downloads all pages listed as `missing_pages` for all tasks in `FETCHING` or `PARTIAL` states.
4.  **Reconciliation:** After execution, verifies which pages were successfully saved to the database and updates each task's status to `COMPLETE` or `PARTIAL`.

**Consequences:**
*   **Positive:**
    *   **Maximum Fault Tolerance:** The process can be interrupted at any time and resumed without data loss by simply re-running the `extract` command.
    *   **Persistent State:** The control table serves as the single source of truth for progress, making the system transparent and easy to debug.
    *   **Efficiency:** The execution phase can focus exclusively on the bulk download of pages, without complex state logic in the main loop.
    *   **Scalability:** The discovery of procurement modalities was easily integrated by generating distinct tasks for each one.
*   **Negative:**
    *   **Increased Complexity:** The code logic is more complex than a simple monolithic script.
    *   **Database Overhead:** Requires more interactions with DuckDB to manage task states.

---

## **ADR-003: Adopt dbt and the Medallion Architecture (Bronze/Silver/Gold)**

**Status:** Accepted

**Context:**
The raw data from the PNCP API is nested, inconsistent, and not ready for analysis. We need a robust, testable, and well-documented framework to transform this data into actionable insights.

**Decision:**
We will use **dbt (Data Build Tool)** to manage the entire data transformation lifecycle. The dbt models will be organized following the **Medallion Architecture**:
1.  **Bronze (`bronze`):** Tables that contain the raw data parsed from the JSON column, with basic typing and minimal parsing. This is a direct source from the `psa.pncp_raw_responses` table.
2.  **Silver (`silver`):** Models representing clean, normalized business entities (contracts, procurements, items, etc.). Data is enriched, deduplicated, and conformed. Surrogate keys are generated.
3.  **Gold (`gold`):** Business-ready, aggregated data marts. These are tables or views optimized for specific analytical use cases, such as `mart_procurement_analytics`.

**Consequences:**
*   **Positive:**
    *   **Clear Data Lineage:** The Bronze/Silver/Gold structure makes the data flow explicit and easy to understand.
    *   **Quality and Testability:** dbt allows for the creation of data tests (e.g., `unique`, `not_null`) that ensure the integrity of the models.
    *   **SQL-based Development:** Transformation logic is written in SQL, a widely known and accessible language.
    *   **Automatic Documentation:** dbt generates a website with complete project documentation, including a DAG of dependencies between models.
    *   **Reproducibility:** The transformation process is deterministic and can be re-run at any time.
*   **Negative:**
    *   **New Dependency:** Adds dbt as a key dependency to the project.
    *   **Learning Curve:** The team needs to be familiar with dbt concepts and best practices.

---

## **ADR-004: End-to-End (E2E) Only Testing Strategy**

**Status:** Accepted

**Context:**
BALIZA is an integration system. Its primary value lies in its ability to correctly interact with external systems (the PNCP API, the file system) and ensure the data pipeline works from end to end. Unit tests with mocks could provide a false sense of security, as they would not validate the actual integration.

**Decision:**
The project's testing strategy will focus **exclusively on End-to-End (E2E) tests**. There will be no unit or integration tests that use mocks to validate business logic. Validation will be performed by running the pipeline against the real PNCP API on a controlled scope (e.g., a single day of data).

**Consequences:**
*   **Positive:**
    *   **Maximum Confidence:** A successful E2E test proves that the system *actually* works under production-like conditions.
    *   **Detects Integration Issues:** API contract errors, unexpected data format changes, and authentication issues are caught immediately.
    *   **Simplified Maintenance:** Less test code to write and maintain, without the complexity of managing mocks.
    *   **Focus on Value:** Tests validate user workflows and business outcomes, not implementation details.
*   **Negative:**
    *   **Slowness:** E2E tests are inherently slower than unit tests.
    *   **Flakiness:** Tests depend on the availability and consistency of the external API and can fail for reasons outside our control (e.g., network instability). This will be mitigated with retry logic (`tenacity`).
    *   **Harder Debugging:** A failure in an E2E test can be more difficult to trace back to the exact root cause in the code.

---

## **ADR-005: Adopt a Modern, High-Performance Python Toolchain**

**Status:** Accepted

**Context:**
For an I/O-intensive data processing project like BALIZA, performance and developer experience (DX) are crucial. Traditional tools like `pip`, `virtualenv`, and the `black`/`isort`/`flake8` combination work, but more modern alternatives offer significant gains.

**Decision:**
We will adopt a modern, integrated set of tools for Python development:
1.  **`uv`:** For dependency and virtual environment management, replacing `pip` and `venv`.
2.  **`ruff`:** For code linting and formatting, replacing `black`, `isort`, `flake8`, and others.
3.  **`httpx` with HTTP/2 support:** For asynchronous HTTP requests, instead of `requests`.
4.  **`typer` and `rich`:** To create a modern, interactive, and user-friendly Command Line Interface (CLI).

**Consequences:**
*   **Positive:**
    *   **Development Performance:** `uv` is orders of magnitude faster at installing and resolving dependencies, speeding up environment setup and CI pipelines.
    *   **Simplified DX:** `ruff`, as a single, extremely fast tool, simplifies the `pre-commit` configuration and shortens feedback cycles.
    *   **Execution Performance:** `httpx` with HTTP/2 allows for more efficient use of network connections, which is critical for bulk data extraction.
    *   **Usability:** The CLI built with `typer` and `rich` provides a superior user experience with automatic help, validation, and informative progress bars.
*   **Negative:**
    *   **Newer Tools:** `uv` is newer than `pip`. Although stable, it may have less history in niche scenarios.
    *   **Learning Curve:** Developers accustomed to the traditional ecosystem may need a brief adjustment period.

---

## **ADR-006: Publish Data to the Internet Archive**

**Status:** Accepted

**Context:**
BALIZA's mission is to create an "Open Backup." This implies not only collecting the data but also making it publicly available in a permanent and free manner. We need a hosting platform that aligns with these values.

**Decision:**
The **Internet Archive (IA)** will be the primary platform for publishing the datasets generated by BALIZA (the DuckDB file and Parquet exports). The daily CI/CD workflow will be responsible for uploading the updated data.

**Consequences:**
*   **Positive:**
    *   **Mission Alignment:** The IA is a non-profit dedicated to digital preservation, which aligns perfectly with BALIZA's goal.
    *   **Zero Cost and Permanence:** The IA offers free, long-term storage.
    *   **Accessibility:** Data becomes globally accessible via direct URLs, enabling features like remote querying of the DuckDB database.
    *   **Versioning:** The IA maintains a history of file versions, contributing to traceability.
*   **Negative:**
    *   **Speed:** Upload/download speeds may be lower than those of commercial cloud providers like AWS S3 or GCS.
    *   **API:** The IA's management API is less flexible than those of commercial services.
    *   **Dependency:** The project becomes dependent on the continuity and policies of the Internet Archive.

---

## **ADR-007: Implement an MCP Server for AI-Powered Analysis**

**Status:** Accepted

**Context:**
The structured data in the Gold layer is extremely valuable, but exploring it still requires technical knowledge (SQL, Python). To further democratize access and enable more intuitive forms of analysis, we can expose the data to Large Language Models (LLMs).

**Decision:**
We will implement a server compliant with the **Model Context Protocol (MCP)**, an open standard from Anthropic. This server (`baliza mcp`) will connect to the local DuckDB database and expose "tools" (like `execute_sql_query`) that an LLM (such as Claude) can use to autonomously and securely query the data on behalf of a user.

**Consequences:**
*   **Positive:**
    *   **Natural Language Querying:** Allows users to ask complex questions about the data without writing a single line of SQL.
    *   **AI-Augmented Analysis:** Unlocks advanced use cases like automatic summary generation, anomaly detection, and visualization creation by an LLM.
    *   **Security:** The implementation ensures that only read-only (`SELECT`) queries are permitted, protecting the integrity of the database.
    *   **Innovation:** Positions BALIZA at the forefront of the interaction between open data and artificial intelligence.
*   **Negative:**
    *   **Additional Component:** Adds a new service to maintain and document.
    *   **Dependency on an External Standard:** The success of the feature depends on the adoption and stability of the MCP standard.
    *   **Resources:** Running the MCP server consumes local resources (CPU/memory) while active.

---

## **ADR Summary**

These architectural decisions form the foundation of BALIZA's design philosophy:

1. **DuckDB-centric** data architecture for portability and performance
2. **Resilient, task-driven** extraction with fault tolerance
3. **Medallion architecture** with dbt for data transformation
4. **E2E-only testing** strategy for maximum confidence
5. **Modern Python toolchain** for developer experience and performance
6. **Internet Archive** for open data publishing
7. **MCP server** for AI-powered data access

Each decision balances technical requirements with the project's mission of creating an accessible, open backup of Brazilian public procurement data.