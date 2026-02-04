# Baliza CLI

[![Extraction](https://github.com/franklinbaldo/baliza/actions/workflows/continuous-extract.yml/badge.svg)](https://github.com/franklinbaldo/baliza/actions/workflows/continuous-extract.yml)
[![Backfill](https://github.com/franklinbaldo/baliza/actions/workflows/historical-backfill.yml/badge.svg)](https://github.com/franklinbaldo/baliza/actions/workflows/historical-backfill.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Dashboard](https://img.shields.io/badge/Dashboard-Live-green)](https://franklinbaldo.github.io/baliza/)

**Baliza** (Backup Aberto de Licitações Zelando pelo Acesso) is an open-source **command-line tool** that captures contract data from the Brazilian National Public Procurement Portal (PNCP) and stores it in a **DuckDB** database ready for analysis. The project was created to preserve the history of Brazilian public procurement and provide a consistent base for journalists, researchers, and oversight bodies.

> **⚠️ This repository contains only the data extraction CLI.**
> For visualization, dashboards, and web interface, see the `baliza-site` project. Full architecture documentation in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Overview

- **Direct Extraction with HTTPX:** Baliza uses `httpx` for direct and resilient calls to the PNCP `GET /v1/contratos` endpoint.
- **Lean CLI:** The `baliza extract` command performs extraction by period.
- **DuckDB Storage:** Raw data is stored in a local `baliza.duckdb` database for easy access and analysis.
- **Parquet Export:** `baliza export` and `baliza export-daily` generate optimized files for external consumption.
- **Resilience:** Support for checkpoints to resume interrupted extractions.
- **State Management:** Commands to inspect extraction status and coverage gaps.

## Installation

### Option 1: Direct Execution with uvx (Recommended)

Run Baliza without cloning the repository:

```bash
# Run directly from GitHub
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza --help

# Example usage
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza extract --start 2023-01-01 --end 2023-01-02
```

### Option 2: Local Installation

```bash
# Clone repository
git clone https://github.com/franklinbaldo/baliza.git
cd baliza

# Install dependencies
uv sync --all-extras

# Run
uv run baliza extract --start 2023-01-01 --end 2023-01-02
```

## Available Commands

| Command | Description |
|---------|-----------|
| `baliza extract` | Extracts data from PNCP to DuckDB (requires `--start` and `--end`). Supports automatic resume via checkpoint. |
| `baliza verify` | Verifies data coverage and detects gaps in the specified period. |
| `baliza state show` | Displays overall status of extraction and buffer. (Alias for `baliza status`) |
| `baliza state gaps` | Lists identified coverage gaps. (Alias for `baliza verify`) |
| `baliza state buffer` | Displays local database buffer statistics. (Alias for `baliza buffer-stats`) |
| `baliza export` | Exports a full DuckDB table to a Parquet file. |
| `baliza export-daily` | Exports a daily package (contracts + organizations + metadata) partitioned by date. |

### Examples

```bash
# Extract data for a period
baliza extract --start 2023-10-01 --end 2023-10-05

# Check for gaps
baliza state gaps --start 2023-10-01 --end 2023-10-05

# Export daily package
baliza export-daily --date 2023-10-01
```

## Running Tests

Baliza uses BDD (Behavior Driven Development) with `pytest-bdd`.

```bash
# Run all tests
uv run pytest

# Run only Tier 0 (Critical Path) tests
uv run pytest -m tier0

# Run smoke tests
uv run pytest -m smoke
```

## Repository Structure

```
├── src/baliza/
│   ├── cli_simple.py       # Command Line Interface (Typer)
│   ├── extractor.py        # Extraction logic with httpx and DuckDB
│   ├── daily_exporter.py   # Daily data export logic
│   └── utils.py            # Helper functions (validation, security)
├── docs/                   # Documentation
├── tests/                  # Automated tests
└── pyproject.toml          # Metadata and dependencies
```

## Contributing

1. Open an issue describing the problem or desired improvement.
2. Create a fork and a branch based on `main`.
3. Run relevant tests (`uv run pytest`) before opening the PR.
4. Clearly describe the impact of changes and update documentation.

## License

Baliza is distributed under the [MIT](LICENSE) license.
