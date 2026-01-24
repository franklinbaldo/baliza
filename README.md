# Baliza CLI

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

**Baliza** (Backup Aberto de Licitações Zelando pelo Acesso) is an open-source **command-line tool** that extracts public procurement data from the Brazilian National Public Procurement Portal (PNCP) and stores it in a local **DuckDB** database, ready for analysis.

The project's goal is to preserve a long-term, reliable archive of Brazilian public procurement data and make it accessible for analysis by journalists, researchers, and civil society.

> **⚠️ This repository contains the data extraction CLI only.**
> For the web interface, dashboards, and visualizations, see the `baliza-site` project (coming soon).

## Overview

- **Simple and Robust Extraction:** The `baliza extract` command uses `httpx` to fetch data from the PNCP's `GET /v1/contratos` endpoint for a specified date range.
- **Local First:** Data is stored in a local `baliza.duckdb` file, giving you full ownership and control over the data.
- **Resumable by Design:** The extractor saves its progress after each page, so if the process is interrupted, it can be resumed without losing data.
- **Ready for Analysis:** The data is stored in a clean, structured format, ready for analysis with DuckDB, Pandas, or any other tool that can read DuckDB files.
- **Export to Parquet:** The `baliza export` command can dump data from any table into a single Parquet file, and `baliza export-daily` creates a curated daily data package.

## Installation

### Option 1: Running with `uvx` (Recommended)

You can run Baliza directly from GitHub without cloning the repository:

```bash
# Run the --help command to see available options
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza --help

# Example: Extract data for a specific date range
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza extract --start 2024-01-01 --end 2024-01-31
```

**Advantages:**
- ✅ No need to clone the repository.
- ✅ Always uses the latest version from the `main` branch.
- ✅ Creates an isolated environment automatically.

### Option 2: Local Development Setup

Clone the repository for local development:

```bash
# Clone the repository
git clone https://github.com/franklinbaldo/baliza.git
cd baliza

# Install dependencies
uv sync

# Run the CLI
uv run baliza extract --start 2024-01-01 --end 2024-01-31
```

### Requirements

- Python 3.11 or higher.
- [uv](httpss://github.com/astral-sh/uv) installed (`curl -LsSf https://astral.sh/uv/install.sh | sh`).
- Internet access to connect to the PNCP API.

## Quickstart

The main command is `extract`, which requires a start and end date.

```bash
# Extract data for the first week of January 2024
uv run baliza extract --start 2024-01-01 --end 2024-01-07
```

This command will create a `baliza.duckdb` file in your current directory.

### Inspecting the Data

You can open the generated DuckDB file to inspect the data:

```bash
# Open the database with the DuckDB CLI
duckdb baliza.duckdb

# Or run a query directly
duckdb baliza.duckdb "SELECT COUNT(*) FROM baliza_raw.contratos;"
```

You can also use it with Python libraries like Pandas:

```python
import duckdb

con = duckdb.connect("baliza.duckdb")
df = con.execute("SELECT * FROM baliza_raw.contratos").df()
print(df.head())
```

## Available Commands

### `extract`
Extracts data from the PNCP for a given date range.

- **`--start` / `--end`**: (Required) The date range in `YYYY-MM-DD` format.
- **`--duckdb`**: Path to the DuckDB file (defaults to `baliza.duckdb`).
- **`--resource`**: The API resource to extract (defaults to `contratos`).

### `export`
Exports a table to a single Parquet file.

- **`--table`**: (Required) The name of the table to export.
- **`--output`**: (Required) The path to the output directory.

### `export-daily`
Creates a curated daily data package in a date-stamped folder, including contracts, organizations, and metadata.

- **`--date`**: (Required) The date to export in `YYYY-MM-DD` format.
- **`--output`**: The parent directory for the export (defaults to `data/daily`).

### `verify`
Checks for gaps in the downloaded data for a given date range.

- **`--start` / `--end`**: (Required) The date range to verify.

### `status`
Displays a summary of the current state of the local database, including total contracts and date ranges.

## Repository Structure

```
├── src/baliza/
│   ├── cli_simple.py       # Command-line interface (Typer)
│   ├── extractor.py        # Core data extraction logic (httpx)
│   ├── daily_exporter.py   # Logic for the `export-daily` command
│   └── utils.py            # Helper functions
├── tests/
│   ├── features/           # BDD feature files
│   └── step_defs/          # BDD step definitions
└── pyproject.toml          # Project metadata and dependencies
```

## Contributing

1.  Open an issue to discuss the proposed change.
2.  Create a fork and a new branch.
3.  Run the test suite with `uv run pytest`.
4.  Update the documentation if needed and open a Pull Request.

## License

Baliza is distributed under the [MIT License](LICENSE).
