# Project Goals for Baliza CLI

This document defines the primary goals, scope boundaries, and target users for the Baliza CLI project, as inferred from the repository evidence.

## Primary Goals

1.  **Provide Reliable and Resumable Data Extraction:** To create a robust, fault-tolerant command-line tool that reliably extracts public procurement data from the Brazilian National Public Procurement Portal (PNCP). The extraction process must be resumable to handle network failures and interruptions gracefully.

2.  **Ensure Data Accessibility for Analysis:** To make the captured data immediately available and useful for analysis by storing it in open, standard formats (DuckDB and Parquet). This empowers users to perform complex queries and build analytical models without needing to handle the complexities of API interaction.

3.  **Preserve a Verifiable Historical Archive:** To build and maintain a complete, auditable, and long-term historical record of Brazilian public procurement data. The tool must include mechanisms to verify data integrity and identify coverage gaps.

## Non-Goals (Scope Boundaries)

-   **Data Visualization and Web Interfaces:** This project is strictly a command-line tool for data extraction and processing. All user-facing dashboards, visualizations, and web interfaces are the responsibility of the separate `baliza-site` project.
-   **Performing Analysis:** The tool's purpose is to *provide* data for analysis, not to perform the analysis itself. It does not include features for generating reports, insights, or visualizations.
-   **Real-time Data Streaming:** The data extraction process is designed for periodic, batch-oriented execution, not for real-time, low-latency data streaming.

## Primary Users

-   **Journalists:** Investigating public spending and procurement patterns.
-   **Researchers:** Studying public administration, economics, and law.
-   **Oversight Bodies:** Monitoring government contracts for compliance and efficiency.
-   **Data Analysts:** Requiring a clean, structured dataset of public procurement for various analytical tasks.

## Success Signals

-   The CLI successfully extracts data from the PNCP and populates a local DuckDB database.
-   The extraction process can automatically resume and recover from interruptions.
-   The `baliza verify` command confirms high data coverage with no unexplained gaps.
-   Users can successfully export data to Parquet and use it in common data analysis tools.

---
*Confidence Level: High. This assessment is based directly on the project's `README.md`.*
