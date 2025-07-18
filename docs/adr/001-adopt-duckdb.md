# ADR-001: Adopt DuckDB as the Primary Database

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