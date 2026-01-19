# Project Goals

This document outlines the primary goals, non-goals, and users for the Baliza project.

## Goals

1.  **Preserve Public Procurement History:** Capture and store historical data of Brazilian public procurement from the PNCP (Portal Nacional de Contratações Públicas).
2.  **Provide a Robust Extraction Pipeline:** Offer a reliable, resumable, and incremental data extraction process that can handle failures and gaps.
3.  **Enable Data Analysis:** Store the extracted data in accessible formats (DuckDB and Parquet) to facilitate analysis for journalists, researchers, and oversight bodies.

## Non-Goals

-   **Data Visualization:** The Baliza CLI is focused solely on data extraction. Visualization is handled by the separate `baliza-site` project.
-   **Extraction from other PNCP endpoints:** The current scope is limited to the `contratos` endpoint.

## Primary Users

-   **Journalists:** Investigating public spending.
-   **Researchers:** Studying public administration and procurement patterns.
-   **Oversight Bodies:** Monitoring and auditing public contracts.

## Success Signals

-   The extracted data is complete, accurate, and up-to-date.
-   The CLI is easy to use and well-documented.
-   The extraction pipeline is resilient to network failures and API interruptions.
