# Project Goals: Baliza CLI

This document defines the primary goals, non-goals, and target users for the Baliza CLI project.

*Confidence Level: High*

## Goals

1.  **Preserve Public Procurement History:** The primary goal is to create a reliable, independent archive of Brazilian public procurement data from the Portal Nacional de Contratações Públicas (PNCP).
2.  **Provide a Robust Extraction Pipeline:** Offer a simple, resumable, and fault-tolerant command-line tool (`baliza extract`) to capture contract data. The pipeline must handle incremental updates, detect and fill gaps, and recover from interruptions.
3.  **Enable Local Data Analysis:** Store the extracted data in a local DuckDB database, making it immediately accessible for analysis. Provide a command (`baliza export`) to convert the data into partitioned Parquet files, a standard format for data science and analytics workflows.

## Non-Goals

-   **Data Visualization:** The Baliza CLI is strictly a data extraction and storage tool. Dashboards, web interfaces, and data visualizations are the responsibility of the separate `baliza-site` project.
-   **Complex Data Transformation:** The tool focuses on capturing the raw data as provided by the PNCP API. It does not perform complex cleaning, normalization, or transformation beyond what is necessary for basic storage and partitioning.
-   **Extraction from non-`contratos` endpoints:** The current scope is limited to the `/v1/contratos` endpoint. While other endpoints may be added in the future, they are not a current priority.

## Primary Users

-   **Journalists & Researchers:** Individuals who need a reliable, local copy of PNCP data to conduct investigations and analysis without relying on the availability or performance of the official portal.
-   **Public Oversight Bodies:** Organizations and civic groups that monitor government spending and need a consistent, verifiable dataset.
-   **Data Engineers & Analysts:** Professionals who need to integrate Brazilian public procurement data into larger data workflows and analytical pipelines.
