# Baliza Project Goals

This document outlines the primary goals, non-goals, and target users for the Baliza project, based on evidence from the repository.

## Primary Goals

- **Reliable Data Extraction:** The core goal of Baliza is to reliably extract public procurement data (currently focused on **contracts**) from the PNCP (Portal Nacional de Contratações Públicas).
- **Data Preservation:** The project aims to create a preserved, long-term archive of Brazilian public procurement data, storing it in a format suitable for analysis (DuckDB and Parquet).
- **Accessibility for Analysis:** Baliza is designed to make this data easily accessible for journalists, researchers, and public oversight bodies through standardized export formats.
- **Operational Transparency:** Providing tools to audit coverage (`verify`), monitor status (`status`, `buffer-stats`), and ensure data integrity.

## Non-Goals

- **Web Interface/Visualization:** The Baliza CLI project explicitly does not include a web interface, dashboards, or data visualization tools. These are planned for a separate project, `baliza-site`.
- **Real-time Data Processing:** The project is designed for batch extraction and backfilling, not real-time data streaming.
- **Data Modification:** Baliza is focused on extracting and storing the data as-is from the PNCP. It does not aim to clean, modify, or enrich the data beyond what is necessary for storage and partitioning.

## Primary Users

- **Journalists:** A primary audience for this data is journalists investigating public spending and government contracts.
- **Researchers:** Academics and researchers in fields like public policy, economics, and law can use this data for their studies.
- **Oversight Bodies:** Government and non-governmental organizations focused on transparency and accountability can use this data to monitor public procurement.

## Success Signals

- **Completeness and Accuracy:** The data in the Baliza database accurately reflects the data available in the PNCP for the covered time periods.
- **Ease of Use:** Users can easily install and run the Baliza CLI to extract and export the data they need.
- **Resilience:** The extraction process handles common issues like network failures and API interruptions gracefully.

## Confidence

High. The current implementation in `extractor.py` and `cli_simple.py` aligns with these goals.
