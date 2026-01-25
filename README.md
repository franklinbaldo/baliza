# Baliza CLI

[![Tests](https://github.com/franklinbaldo/baliza/actions/workflows/ci.yml/badge.svg)](https://github.com/franklinbaldo/baliza/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

**Baliza** is a command-line tool for extracting public procurement data from Brazil's National Public Procurement Portal (PNCP) and saving it to a local DuckDB database. Its primary goal is to provide a reliable, transparent, and accessible way to preserve this important historical data for journalists, researchers, and public oversight.

> **⚠️ This repository contains the data extraction CLI only.**
> For the web interface and dashboards, see the separate `baliza-site` project (coming soon).

## Key Features

- **Simple & Direct Extraction:** Uses `httpx` for efficient, direct API calls to the PNCP.
- **Local Analytics with DuckDB:** All data is stored in a local `baliza.duckdb` file, ready for immediate analysis.
- **Page-Level Checkpointing:** The `extract` command is resumable. If it's interrupted, it can continue from the last successfully fetched page, preventing data loss and re-work.
- **Data Export:** Easily export data to Parquet files for use in other analytics tools.
- **Daily Export Packages:** Create self-contained daily data packages, including contracts and related entities, for archival and distribution.

## Installation

### Recommended: `uvx` (Direct Execution)

Run Baliza without cloning the repository using `uvx`. This ensures you're always using the latest version.

```bash
# Get help text
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza --help

# Example: Extract data for a specific date range
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza extract --start 2024-01-01 --end 2024-01-02
```

### Local Development

Clone the repository and install dependencies using `uv`.

```bash
git clone https://github.com/franklinbaldo/baliza.git
cd baliza
uv sync --all-extras  # Install main and test dependencies
uv run baliza --help
```

### Requirements

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) installed (`curl -LsSf https://astral.sh/uv/install.sh | sh`)

## Quickstart

The main command is `baliza extract`, which requires a start and end date.

```bash
# Extract data for a two-day period
uv run baliza extract --start 2024-01-01 --end 2024-01-02

# Check the status of your local database
uv run baliza status

# Export the 'contratos' table to a Parquet file
uv run baliza export --table contratos --output data/

# Create a daily export package for a specific date
uv run baliza export-daily --date 2024-01-01 --output data/daily
```

The `baliza extract` command will create or update a `baliza.duckdb` file in your current directory. This file contains the extracted data and the state tables used for checkpointing.

## Available Commands

| Command | Description |
|---|---|
| `extract` | Extracts data for a given date range with page-level checkpointing. |
| `status` | Shows a summary of the local database, including total records and date range. |
| `export` | Exports a specific table to a single Parquet file. |
| `export-daily`| Creates a self-contained daily data package for archival. |
| `verify` | (Experimental) Audits data coverage to find gaps. |

Use `baliza <command> --help` to see all available options for each command.

## Inspecting the Data

You can connect to the generated DuckDB file to analyze the data directly.

```python
import duckdb

# Connect to the database
con = duckdb.connect("baliza.duckdb")

# Query the contracts table
df = con.execute("SELECT * FROM baliza_raw.contratos LIMIT 10").df()
print(df)

# Check extraction status
checkpoints = con.execute("SELECT * FROM baliza_state.extraction_checkpoint").df()
print(checkpoints)
```

## How Extraction Works

The `PNCPExtractor` class in `src/baliza/extractor.py` manages the entire process:
1. It sends a request to the PNCP API for a given date range.
2. The API responds with the first page of data and the total number of pages.
3. For each page fetched, the data is immediately inserted into the `baliza_raw.contratos` table in DuckDB.
4. After each successful page insertion, a record is saved in the `baliza_state.extraction_checkpoint` table.
5. If the process is interrupted, running the same `extract` command again will read the checkpoint and resume from the next page.
6. Once all pages for the date range are successfully extracted, the checkpoint is cleared.

## Running Tests

This project uses `pytest` and `pytest-bdd`. To run the test suite:

```bash
# Install all dependencies
uv sync --all-extras

# Run tests
uv run pytest
```

## Contributing

1. Open an issue to discuss the proposed change.
2. Fork the repository and create a new branch.
3. Make your changes and ensure the tests pass (`uv run pytest`).
4. Open a pull request and link it to the issue.

## License

Baliza is distributed under the [MIT License](LICENSE).
