# Baliza CLI

**Baliza** (Backup Aberto de Licitações Zelando pelo Acesso) is an open-source
**command-line tool** that extracts public procurement data from the Portal
Nacional de Contratações Públicas (PNCP) and stores it in a **DuckDB** database,
ready for analysis. The project's goal is to preserve the history of Brazilian
public procurement and provide a reliable foundation for journalists,
researchers, and civil society organizations.

> **⚠️ This repository contains the data extraction CLI only.**
> For a web interface, dashboards, and visualizations, see the `baliza-site`
> project (coming soon).

## Project Goals

- **Reliable Data Extraction:** Provide a simple and robust CLI to extract data
  from the PNCP API into a local DuckDB database.
- **Data Preservation:** Ensure raw, unmodified data is stored for long-term
  analysis and reproducibility.
- **Data Accessibility:** Enable easy data export to common analytical formats
  like Parquet.

## Current Status

The Baliza CLI is currently in a **stable alpha** phase. The core extraction
logic has been simplified to use a direct `httpx`-to-DuckDB pipeline, replacing
the previous `dlt`-based implementation.

**What works today:**
- Extracting `contratos` (contracts) for a specified date range.
- Verifying data coverage to find gaps.
- Exporting tables to Parquet.

**What's next:**
- Re-implementing robust BDD tests for the new architecture.
- Expanding coverage to other PNCP endpoints (e.g., `contratacoes`).
- See `docs/MASTERPLAN.md` for the complete roadmap.

## Installation

### Option 1: Direct Execution with `uvx` (Recommended)

Run Baliza without cloning the repository:

```bash
# Run directly from GitHub
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza --help

# Example: Extract the first week of 2024
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza extract \
  --start 2024-01-01 \
  --end 2024-01-07
```

### Option 2: Local Development

Clone the repository for local development:

```bash
# Clone the repository
git clone https://github.com/franklinbaldo/baliza.git
cd baliza

# Install dependencies
uv sync --all-extras

# Run the CLI
uv run baliza extract --start 2024-01-01 --end 2024-01-07
```

## Quickstart

The `extract` command requires a start and end date and creates (or updates) a
`baliza.duckdb` file in the current directory.

```bash
# Extract data for a specific date range
uv run baliza extract --start 2024-03-01 --end 2024-03-31

# Verify coverage for the extracted period
uv run baliza verify --start 2024-03-01 --end 2024-03-31

# Export the 'contratos' table to a Parquet file
uv run baliza export --table contratos --output data/
```

## Available Commands

| Command | Description |
|---------|-------------|
| `extract` | Extracts data from the PNCP API for a given date range. |
| `verify`  | Checks the local database for gaps in a given date range. |
| `export`  | Exports a database table to a Parquet file. |

Use `uv run baliza [COMMAND] --help` to see all available options.

## Inspecting the Data

You can query the generated `baliza.duckdb` file using any DuckDB-compatible
tool or library.

**CLI:**
```bash
uv run python -m duckdb baliza.duckdb
```
```sql
-- Inside DuckDB shell
USE baliza_raw;
SELECT COUNT(*) AS total_contratos,
       MAX(dataAtualizacao) AS ultima_atualizacao
FROM contratos;
```

**Python:**
```python
import duckdb

con = duckdb.connect("baliza.duckdb")
df = con.execute("SELECT * FROM baliza_raw.contratos").df()
print(df.head())
```

## Repository Structure

```
├── src/baliza/
│   ├── cli_simple.py       # CLI command definitions (Typer)
│   └── extractor.py        # Core extraction logic (httpx -> DuckDB)
├── docs/
│   └── MASTERPLAN.md       # Project goals, roadmap, and architecture
├── tests/
│   ├── features/           # BDD feature files
│   └── step_defs/          # BDD step definitions
└── pyproject.toml          # Dependencies and project metadata
```

## Contributing

Contributions are welcome! Please open an issue to discuss your idea before
submitting a pull request.

## License

Baliza is distributed under the [MIT](LICENSE) license.
