# Project Goals

This document outlines the primary goals, non-goals, and success metrics for the Baliza project, based on an analysis of the codebase and existing documentation.

## Primary Goals

1.  **Reliable Data Extraction**: Continuously and reliably extract public procurement data from the PNCP (Portal Nacional de Contratações Públicas) API. The core function is to capture specific data resources (like `contratos`) for a given time range.
2.  **Data Preservation**: Store the extracted raw data in a durable, queryable format (DuckDB) to create a long-term, verifiable archive of public contracts.
3.  **Data Accessibility**: Provide simple, command-line tools to export the preserved data into a common analytical format (Parquet) for use by journalists, researchers, and oversight bodies.

## Non-Goals

-   **Data Visualization**: The project is not responsible for building dashboards, charts, or a web interface. This is handled by a separate `baliza-site` project.
-   **Real-time Data Streaming**: The extraction process is batch-oriented, focused on daily or historical data, not real-time event streams.
-   **Complex Data Transformation**: The tool focuses on capturing raw data as-is ("bronze" layer). Complex cleaning, normalization, or joining (silver/gold layers) are out of scope for this tool.

## Primary Users

-   **Data Journalists & Researchers**: Need a reliable, local copy of PNCP data to conduct investigations without depending on the availability or rate limits of the official API.
-   **Developers & Data Engineers**: Need a simple, scriptable tool to integrate into larger data pipelines or analytical workflows.

## Success Signals

-   The CLI can successfully extract data for a given date range and store it in a DuckDB file.
-   The CLI can export a specified table from DuckDB to Parquet files.
-   The BDD test suite accurately reflects the tool's capabilities and runs successfully.
-   The `README.md` is an accurate and reliable guide for new users.
