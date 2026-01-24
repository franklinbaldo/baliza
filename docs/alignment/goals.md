# Baliza Project Goals

## Goals

1.  **Reliably extract public procurement data** from the PNCP (Portal Nacional de Contratações Públicas).
2.  **Create a preserved, long-term archive** of the extracted data using DuckDB for local storage and Parquet for bulk export.
3.  **Provide a simple command-line interface (CLI)** to make the data accessible for analysis and integration with other tools.

## Non-Goals

-   **Complex, automated state management:** The current tool does not support automatic gap detection, resumable downloads, or backfills as described in the outdated `README.md`.
-   **Data visualization and web interface:** This is explicitly out of scope and belongs to the `baliza-site` project.
-   **Real-time data streaming:** The tool is designed for batch extraction, not continuous data streams.

## Primary Users

-   Data journalists
-   Researchers
-   Public sector auditors and controllers

## Success Signals

-   The CLI successfully extracts data for a given date range without errors.
-   The extracted data is stored correctly in the DuckDB database.
-   The `export` command produces valid Parquet files.
-   The BDD test suite accurately reflects the current, simplified functionality and passes reliably.
