# Baliza Project Goals

This document outlines the primary goals, non-goals, and success signals for the Baliza project, based on evidence from the repository.

## Goals

1.  **Preserve Public Data:** Capture and preserve the history of Brazilian public procurement data from the PNCP.
2.  **Provide Reliable Access:** Offer a consistent and reliable local database (DuckDB) for journalists, researchers, and auditors.
3.  **Ensure Data Integrity:** Implement a resilient and resumable extraction pipeline that can detect and fill gaps in the data.

## Non-Goals

-   **Real-time Data:** The project is focused on creating a historical archive, not a real-time data feed.
-   **Web-based UI:** This repository is for the CLI tool only. A separate project (`baliza-site`) will handle the web interface.
-   **Advanced Analytics:** The project provides the data in a queryable format but does not include advanced analytical tools.

## Primary Users

-   Journalists
-   Researchers
-   Auditors and public oversight bodies

## Success Signals

-   A complete and verifiable local copy of the PNCP "contratos" endpoint.
-   A CLI that is easy to install and run in a variety of environments (local, CI, Docker).
-   A resilient pipeline that can recover from transient errors and be safely rerun.

## Confidence

High. The project's goals are clearly articulated in the `README.md` and supported by the features implemented in the codebase.
