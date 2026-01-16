# Project Goals

This document lists the high-level project goals, derived from the `MASTERPLAN.md`. It serves as the authoritative source for goal alignment in all feature development.

### North Star

To be the most reliable, transparent, and accessible tool for extracting and preserving Brazilian public procurement data from the National Public Procurement Portal (PNCP), enabling accountability and research for journalists, civil society, and government agencies.

### Concrete Goals

1.  **Achieve Full Extraction Resumability:** Implement a robust state management system that makes the extraction process fully resumable and idempotent, recovering gracefully from network failures or API instability. **(✅ Completed)**
2.  **Comprehensive Endpoint Coverage:** Expand beyond the initial `contratos` endpoint to support all relevant PNCP data sources, providing a complete picture of the procurement lifecycle.
3.  **Automated Data Publishing:** Establish a fully automated CI/CD pipeline to extract data, export it to Parquet, and publish versioned, immutable datasets via GitHub Releases.
4.  **Actionable Data Quality Monitoring:** Develop tools to verify data coverage, detect gaps, and provide clear reports on the completeness and integrity of the extracted data.
5.  **Excellent Developer/Operator Experience:** Provide clear documentation, straightforward configuration, and a simple, predictable command-line interface.
