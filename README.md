# Baliza CLI

**Baliza** is a command-line tool for extracting public procurement data from Brazil's National Public Procurement Portal (PNCP). It stores the data in a local **DuckDB** database, making it immediately available for analysis.

The project's primary goal is to provide a simple, reliable, and scriptable tool for journalists, researchers, and developers who need access to PNCP data.

> **Note:** This repository contains the data extraction CLI. A separate project, `baliza-site`, will provide a web interface for data visualization.

## How It Works

- **Simple Extraction**: The `baliza extract` command fetches data from the PNCP's `/v1/contratos` and `/v1/contratacoes` endpoints for a specified date range.
- **Local Storage**: Data is saved to a local DuckDB file (`baliza.duckdb` by default), allowing for easy querying and analysis.
- **Analytical Export**: The `baliza export` command converts tables from DuckDB into Parquet files, a common format for data analysis pipelines.

This tool uses `httpx` for making HTTP requests and `typer` for the command-line interface. It does **not** use the `dlt` library.

## Installation

You will need Python 3.11+ and [uv](https://github.com/astral-sh/uv) installed.

```bash
# Clone the repository
git clone https://github.com/franklinbaldo/baliza.git
cd baliza

# Install dependencies (including test extras)
uv sync --all-extras
```

## Quick Start

All commands are run from the root of the repository.

### 1. Extract Data

Run the `extract` command with a specified start and end date. The `--resource` option can be `contratos` or `contratacoes`.

```bash
# Extract "contratos" for a 5-day period
uv run baliza extract --resource contratos --start 2024-01-01 --end 2024-01-05
```

This command will create a `baliza.duckdb` file in your current directory containing the extracted data.

### 2. Export Data to Parquet

Run the `export` command to save a table from the DuckDB file to Parquet.

```bash
# Export the "contratos" table
uv run baliza export --table contratos --output-dir data/
```

This will create partitioned Parquet files in the `data/contratos/` directory.

### 3. Verify Data (Coming Soon)

The `verify` command is envisioned to check for gaps in the extracted data. This feature is defined in the BDD tests but is not yet fully implemented in the simplified CLI.

## Running Tests

The test suite uses `pytest` and `pytest-bdd`. To run the tests:

```bash
uv run pytest
```

- **BDD Features**: `tests/features/`
- **Step Definitions**: `tests/step_defs/`

## Available Commands

| Command                               | Description                                                                  |
| ------------------------------------- | ---------------------------------------------------------------------------- |
| `baliza extract`                      | Extracts data from a PNCP resource for a given date range.                   |
| `baliza export`                       | Exports a table from DuckDB to partitioned Parquet files.                    |
| `baliza verify`                       | (Planned) Verifies data coverage and identifies gaps.                        |

Use `uv run baliza --help` to see all available commands and options.

## Contributing

Please open an issue to discuss any proposed changes before submitting a pull request.

## License

This project is licensed under the [MIT License](LICENSE).
