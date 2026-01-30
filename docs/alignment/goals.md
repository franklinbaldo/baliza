# Baliza Project Goals

This document outlines the primary goals, non-goals, and target users for the Baliza project, based on evidence from the repository and the strategic MASTERPLAN.

## North Star

To be the most reliable, transparent, and accessible tool for extracting and preserving Brazilian public procurement data from the National Public Procurement Portal (PNCP), enabling accountability and research for journalists, civil society, and government agencies.

## Primary Goals

- **Reliable and Resumable Data Extraction:** The core goal is to reliably extract public procurement data from the PNCP. The process must be resumable and idempotent, handling network failures and API instability gracefully.
- **Data Preservation and Archiving:** Create a preserved, long-term archive of Brazilian public procurement data, storing it in DuckDB (local) and Parquet (archival) formats.
- **Data Accessibility for Analysis:** Make this data easily accessible for journalists, researchers, and public oversight bodies by providing well-structured, partitioned, and documented datasets.
- **Actionable Observability:** Provide clear visibility into the state of the data extraction process, including coverage tracking, gap detection, and run history.

## Non-Goals

- **Web Interface/Visualization:** The Baliza CLI project explicitly does not include a web interface or dashboards. These are handled in the separate `baliza-site` repository.
- **Real-time Data Processing:** The project is designed for batch extraction and backfilling, not real-time streaming.
- **Data Enrichment/Cleaning:** Baliza focuses on extracting data as-is from the PNCP. Extensive data cleaning or enrichment beyond what's needed for storage and partitioning is out of scope for the CLI.

## Success Metrics

- **Completeness:** 100% coverage of the targeted PNCP endpoints for the requested date ranges, verified by the `state gaps` command.
- **Accuracy:** Data in the local store matches the API responses, with integrity verified by page-level hashes (planned).
- **Resilience:** 0% data loss on transient network or API failures, with automatic recovery via checkpoints and resumability.
- **Usability:** A "Zero-Config" experience where `baliza extract` can run autonomously and maintain its own state.

## Target Users

- **Data Journalists:** investigating public spending and government contracts.
- **Public Policy Researchers:** studying procurement trends and effectiveness.
- **Transparency NGOs:** monitoring government accountability and identifying red flags.
- **Government Agencies:** performing internal audits and cross-referencing data.

## Confidence

High. The goals are derived from the current implementation path and the strategic direction outlined in the `MASTERPLAN.md` and `ARCHITECTURE.md`.
