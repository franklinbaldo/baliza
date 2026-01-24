# Baliza CLI

[![CI](https://github.com/franklinbaldo/baliza/actions/workflows/ci.yml/badge.svg)](https://github.com/franklinbaldo/baliza/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

**Baliza** (Backup Aberto de Licitações Zelando pelo Acesso) is an open-source **command-line tool** that captures public procurement data from Brazil's National Public Procurement Portal (PNCP) and stores it in a local **DuckDB** database, ready for analysis and archival. The project's mission is to preserve the history of Brazilian public procurement and provide a reliable data source for journalists, researchers, and accountability groups.

> **⚠️ This repository contains the data extraction CLI only.**
> For the web interface, dashboards, and data visualization, see the `baliza-site` project. The complete project architecture is documented in [`docs/MASTERPLAN.md`](docs/MASTERPLAN.md).

## Project Goals

1.  **Reliable Extraction:** To robustly extract data from the PNCP, handling network errors and API instability gracefully.
2.  **Long-Term Preservation:** To create a permanent, public archive of procurement data by exporting daily snapshots to the Internet Archive.
3.  **Accessibility & Verification:** To make the data easy to access and allow users to verify its completeness.

## Overview

-   **Simple & Robust Extractor:** A straightforward Python-based extractor (`PNCPExtractor`) uses `httpx` to fetch data from the PNCP's `GET /v1/contratos` endpoint.
-   **Local State & Buffer:** A local DuckDB file (`baliza.duckdb`) is used to store raw data and manage the extraction state, with page-level checkpointing to ensure extractions can be resumed if interrupted.
-   **Archival-Ready Exports:** The `baliza export-daily` command creates self-contained, date-based data packages in Parquet format, perfect for uploading to archival repositories.
-   **Simplified CLI:** The command-line interface is designed to be simple and predictable, with clear commands for extracting, verifying, and exporting data.

## Installation

### Option 1: Direct Execution with `uvx` (Recommended)

Run Baliza without cloning the repository:

```bash
# Run the latest version directly from GitHub
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza --help

# Example: Extract data for a specific date range
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza extract \
    --start 2024-01-01 \
    --end 2024-01-02
```

### Option 2: Local Development Setup

Clone the repository to develop locally:

```bash
# Clone the repository
git clone https://github.com/franklinbaldo/baliza.git
cd baliza

# Install dependencies
uv sync

# Run the CLI
uv run baliza extract --start 2024-01-01 --end 2024-01-02
```

### Requirements

-   Python 3.11 or higher
-   [uv](https://github.com/astral-sh/uv) installed (`curl -LsSf https://astral.sh/uv/install.sh | sh`)
-   Internet access to connect to the PNCP API

## Quick Start

The `extract` command requires a start and end date. It fetches data for the specified period and saves it to a local `baliza.duckdb` file.

```bash
# Extract data for the first week of 2024
uv run baliza extract --start 2024-01-01 --end 2024-01-07

# Check the status of the local database
uv run baliza status

# Export data for a specific day into a self-contained package
uv run baliza export-daily --date 2024-01-01 --output data/daily
```

### Inspecting the Data

You can query the `baliza.duckdb` file directly to inspect the results:

```bash
# Open the database with the DuckDB CLI
duckdb baliza.duckdb

-- Run a query to see the total number of contracts
USE baliza_raw;
SELECT COUNT(*) FROM contratos;
```

## CLI Commands

| Command | Description |
|---|---|
| `baliza extract` | Extracts data for a mandatory `--start` and `--end` date range. Resumes automatically if interrupted. |
| `baliza export-daily` | Exports a self-contained daily data package (Parquet files) for a specific `--date`. |
| `baliza verify` | Checks for gaps in data coverage within a given date range in the local database. |
| `baliza status` | Displays a summary of the local database, including the date range covered and total records. |
| `baliza export` | Exports a raw table from DuckDB to a single Parquet file. |

Use `uv run baliza --help` to see all available commands and options.

## Repository Structure

```
├── src/baliza/
│   ├── cli_simple.py       # Main CLI application (Typer)
│   ├── extractor.py        # Core data extraction and state logic (PNCPExtractor)
│   ├── daily_exporter.py   # Logic for the `export-daily` command
│   └── utils.py            # Helper functions
├── docs/
│   └── MASTERPLAN.md       # Project goals, architecture, and backlog
└── tests/
    ├── features/           # BDD feature files
    └── steps/              # BDD step definitions
```

## Contributing

1.  Open an issue to discuss the proposed change.
2.  Fork the repository and create a new branch.
3.  Ensure tests pass by running `uv run pytest`.
4.  Open a pull request and link it to the issue.

## License

Baliza is distributed under the [MIT](LICENSE) license.
