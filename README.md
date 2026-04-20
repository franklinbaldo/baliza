# Baliza CLI

[![Extraction](https://github.com/franklinbaldo/baliza/actions/workflows/continuous-extract.yml/badge.svg)](https://github.com/franklinbaldo/baliza/actions/workflows/continuous-extract.yml)
[![Backfill](https://github.com/franklinbaldo/baliza/actions/workflows/historical-backfill.yml/badge.svg)](https://github.com/franklinbaldo/baliza/actions/workflows/historical-backfill.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

**Baliza** (Backup Aberto de Licitações Zelando pelo Acesso) is an open-source CLI that extracts contract data from the Brazilian public procurement portal (PNCP) and stores it in a DuckDB database ready for analysis.

## Product vision and user journeys

Who Baliza serves and which problem it solves for each persona is documented in [`VISION.md`](VISION.md) — including the six canonical user journeys (B2G supplier, public buyer, journalist, citizen, researcher, developer), the prioritization principle, and the explicit non-goals. The journeys are mirrored as an executable specification under [`web/features/journeys/`](web/features/journeys/); component-level behaviors live in [`web/features/`](web/features/). Red scenarios are intentional: each one is a backlog item. Run `npm run test:bdd:report` (from `web/`) for the current green/wip/planned breakdown.

## Installation

**Run directly (no install):**

```bash
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza --help
```

**Install locally:**

```bash
git clone https://github.com/franklinbaldo/baliza.git
cd baliza
uv sync
uv run baliza --help
```

Requires Python 3.11+.

## Commands

### `extract` — Fetch data from PNCP

```bash
baliza extract --start 2024-01-01 --end 2024-01-31
```

| Flag | Default | Description |
|------|---------|-------------|
| `--start` | required | Start date (YYYY-MM-DD) |
| `--end` | required | End date (YYYY-MM-DD) |
| `--duckdb, -d` | `baliza.duckdb` | Path to DuckDB file |
| `--dataset, -s` | `baliza_raw` | Dataset/schema name |
| `--resource, -r` | `contratos` | PNCP resource to extract |
| `--workers, -w` | `4` | Parallel workers (1–16) |
| `--deadline-minutes` | none | Stop gracefully after N minutes |

Extraction is resumable — interrupted runs pick up from the last checkpoint.

**Tip:** 4–8 workers is optimal. More workers triggers PNCP rate limits and causes slowdowns.

### `verify` — Check coverage and detect gaps

```bash
baliza verify --start 2024-01-01 --end 2024-01-31 --resource contratos
```

### `export` — Export a table to Parquet

```bash
baliza export --table contratos --output ./output/
```

| Flag | Default | Description |
|------|---------|-------------|
| `--table` | required | Table name to export |
| `--output, -o` | required | Output directory |
| `--duckdb` | `baliza.duckdb` | Path to DuckDB file |
| `--dataset` | `baliza_raw` | Dataset/schema name |
| `--date-col` | `dataPublicacao` | Date column for partitioning |

### `export-daily` — Export a daily Parquet package

```bash
baliza export-daily --date 2024-01-15
```

Writes a self-contained directory for the given date with `contratos.parquet`, `orgaos.parquet`, `unidades.parquet`, and `_metadata.json`.

| Flag | Default | Description |
|------|---------|-------------|
| `--date` | required | Date (YYYY-MM-DD) |
| `--output, -o` | `data/daily` | Output directory |
| `--duckdb` | `baliza.duckdb` | Path to DuckDB file |
| `--dataset` | `baliza_raw` | Dataset/schema name |

### `status` — Show extraction status

```bash
baliza status
```

### `buffer-stats` — Show buffer statistics

```bash
baliza buffer-stats
```

## Environment variables

| Variable | Description |
|----------|-------------|
| `BALIZA_LOG_FORMAT` | Set to `json` for structured logging (default: human-readable console output) |
| `BALIZA_ALLOW_PRIVATE_NETWORKS` | Set to `1` to disable SSRF protection (testing only) |

## Development

```bash
uv sync
pytest tests/
```

## License

MIT
