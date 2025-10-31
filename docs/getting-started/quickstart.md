# Quick Start

This guide will help you run your first data extraction with Baliza.

## Using uvx (without cloning)

```bash
# Alias for simplicity (add to your .bashrc or .zshrc)
alias baliza='uvx --from "git+https://github.com/franklinbaldo/baliza" baliza'

# Extract data from the last 3 days
baliza extract

# Export to Parquet
baliza export --table contratos --out data/contratos

# Monthly backfill
baliza backfill 2024-01 2024-03

# Verify coverage
baliza verify
```

## Using local installation

```bash
# Inside the project directory
uv run baliza extract
uv run baliza export --table contratos --out data/contratos
uv run baliza backfill 2024-01 2024-03
```

## Understanding the Commands

### `baliza extract`

The `baliza extract` command creates (or updates) the `baliza.duckdb` file in
the current directory. By default, the execution looks back a few days (lookback)
and opens daily windows `dataInicial`/`dataFinal`, sending paginated requests with
`tamanhoPagina=500` until `totalPaginas` is traversed.

**Example:**

```bash
baliza extract --lookback-days 7
```

This will extract data from the last 7 days.

### `baliza export`

After extraction, `baliza export` reads the table from DuckDB and writes the data
as partitioned Parquet (year/month) to the specified directory.

**Example:**

```bash
baliza export --table contratos --out data/contratos
```

This creates files in the format: `data/contratos/ano=YYYY/mes=MM/*.parquet`

### `baliza backfill`

The `baliza backfill` command processes entire months in sequence, reusing the
same DuckDB. This covers late rectifications and consolidates historical data.

**Example:**

```bash
baliza backfill 2024-01 2024-12
```

This will process all months from January to December 2024.

### `baliza verify`

The `baliza verify` command audits the coverage manifest by calling only the
first page of each window and marking gaps or late growth reported by the API.

**Example:**

```bash
baliza verify --output report.json
```

## Inspecting the Data

### Using DuckDB CLI

Open the generated DuckDB directly from the shell:

```bash
uv run python -m duckdb --batch <<'SQL'
.open baliza.duckdb
USE baliza_raw;
SELECT COUNT(*) AS total_contratos,
       MAX(dataatualizacao) AS ultima_atualizacao
FROM contratos;
SQL
```

### Using Python

You can also use pandas or polars:

```python
import duckdb

con = duckdb.connect("baliza.duckdb")
con.execute("USE baliza_raw")
contratos = con.execute("SELECT * FROM contratos").df()
print(contratos.head())
```

## Common Workflows

### Daily Incremental Updates

For daily updates, run:

```bash
baliza extract
baliza export --table contratos --out data/contratos
```

### Historical Backfill

To backfill historical data:

```bash
# Backfill all of 2024
baliza backfill 2024-01 2024-12

# Export the backfilled data
baliza export --table contratos --out data/contratos
```

### Auditing Coverage

To check for gaps in your data:

```bash
baliza verify --output coverage-report.json
```

## Next Steps

- [Configuration Guide](configuration.md) - Learn about advanced configuration
- [API Reference](../api/cli.md) - Detailed command documentation
- [Architecture](../ARCHITECTURE.md) - Understand the system design
