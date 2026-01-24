# Baliza CLI

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

**Baliza** is a simple **command-line tool** that extracts public procurement data from the Brazilian PNCP (Portal Nacional de Contratações Públicas) and stores it in a local **DuckDB** database, ready for analysis.

The goal of this project is to provide a straightforward way to create a local, preserved archive of public procurement data for journalists, researchers, and auditors.

> **⚠️ This repository contains only the data extraction CLI.**
> For data visualization and a web interface, see the separate `baliza-site` project (coming soon).

## Overview

- **Simple & Direct:** Uses `httpx` to fetch data directly from the PNCP API.
- **Local First:** Stores data in a local `baliza.duckdb` file for easy access and analysis.
- **Typer CLI:** Provides a clean and simple command-line interface.
- **Parquet Export:** Includes a command to export data to Parquet files for use in other analytics tools.

## Installation

To get started with local development, clone the repository and install the dependencies using `uv`.

```bash
# Clone the repository
git clone https://github.com/franklinbaldo/baliza.git
cd baliza

# Install dependencies (including dev/test tools)
uv sync --all-extras

# Run the CLI
uv run baliza --help
```

### Requirements

- Python 3.11 or higher
- [uv](https://github.com/astral-sh/uv) installed

## Quickstart

The main commands are `extract` and `export`.

### 1. Extract Data

You must provide a start and end date for the extraction.

```bash
# Extract data for a specific day
uv run baliza extract --start 2024-10-01 --end 2024-10-01

# Extract data for a date range
uv run baliza extract --start 2024-10-01 --end 2024-10-07
```

This will create a `baliza.duckdb` file in your current directory containing the extracted `contratos` table.

### 2. Export to Parquet

You can export a table from the DuckDB database to a Parquet file.

```bash
uv run baliza export --table contratos --output data/
```

This will create a `data/contratos.parquet` file.

## Inspecting the Data

You can inspect the generated DuckDB file using the DuckDB CLI or your favorite data analysis library.

```bash
# Open the database with the DuckDB CLI
duckdb baliza.duckdb

# Query the data
D USE baliza_raw;
D SELECT COUNT(*) FROM contratos;
```

Or with Python:

```python
import duckdb

con = duckdb.connect("baliza.duckdb")
df = con.execute("SELECT * FROM baliza_raw.contratos").df()
print(df.head())
```

## Available Commands

| Command | Description |
|---|---|
| `baliza extract` | Extracts data from the PNCP for a given date range. |
| `baliza export` | Exports a table from DuckDB to a Parquet file. |
| `baliza verify` | Verifies data coverage for a given date range. |
| `baliza export-daily` | Exports a self-contained daily data package. |
| `baliza status` | Shows the overall extraction status. |

Use `uv run baliza --help` to see all available commands and options.

## Project Structure

```
├── src/baliza/
│   ├── cli_simple.py       # Typer CLI application
│   └── extractor.py        # Core data extraction logic (httpx + duckdb)
├── tests/
│   ├── features/           # BDD feature files
│   └── step_defs/          # BDD step definitions
└── pyproject.toml          # Dependencies and project metadata
```

## Contributing

1. Open an issue to discuss the proposed change.
2. Fork the repository and create a new branch.
3. Make your changes and run the test suite (`uv run pytest`).
4. Open a pull request.

## License

Baliza is distributed under the [MIT](LICENSE) license.
