# Project Goals

- **Goals:**
  - Reliably extract and preserve Brazilian public procurement data from the PNCP.
  - Provide a simple and transparent command-line interface for data extraction.
  - Ensure data is stored in an accessible format (DuckDB, Parquet) for analysis.

- **Non-Goals:**
  - Data visualization or a web interface (this is handled by the separate `baliza-site` project).
  - Complex, fully automated backfilling or gap detection (the current implementation is designed for manual, date-range-based extraction).

- **Primary Users:**
  - Journalists, researchers, and public oversight bodies.

- **Success Signals:**
  - High test coverage for the core extraction logic.
  - An accurate and up-to-date `README.md`.
  - BDD scenarios that clearly reflect and validate real user workflows.

- **Confidence:**
  - High. The current simplified architecture is robust, well-defined, and easier to maintain.
