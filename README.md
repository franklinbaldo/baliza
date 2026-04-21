# Baliza CLI

[![PNCP Sync](https://github.com/franklinbaldo/baliza/actions/workflows/pncp-sync.yml/badge.svg)](https://github.com/franklinbaldo/baliza/actions/workflows/pncp-sync.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

**Baliza** (Backup Aberto de Licitações Zelando pelo Acesso) is an open-source CLI that extracts contract data from the Brazilian public procurement portal (PNCP) and stores it in a DuckDB database ready for analysis.

## Product vision and user journeys

Who Baliza serves and which problem it solves for each persona is documented in [`VISION.md`](VISION.md) — including the seven canonical user journeys (B2G supplier, public buyer, journalist, citizen, researcher, developer, auditor), the prioritization principle, and the explicit non-goals. The journeys are mirrored as an executable specification under [`web/features/journeys/`](web/features/journeys/); component-level behaviors live in [`web/features/`](web/features/). Red scenarios are intentional: each one is a backlog item. Run `npm run test:bdd:report` (from `web/`) for the current green/wip/planned breakdown.

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

Baliza V2 collapses the pipeline into a single omnibus command. The scheduled GitHub Actions workflow runs only `baliza sync`.

### `sync` — Extract missing months, upload to IA, consolidate

```bash
baliza sync --start-date 2023-01-01
```

Walks backwards from the previous month to `--start-date`, skipping months already present on Internet Archive (source of truth is the remote `manifest.csv`). For each missing month it extracts from PNCP, uploads the monthly Parquet snapshot to IA, and — once the run finishes with new data — rebuilds the annual consolidated archives.

| Flag | Default | Description |
|------|---------|-------------|
| `--start-date` | `2023-01-01` | Oldest date to backfill to |
| `--batch-size, -n` | all pending | Max months to sync in one run |
| `--force-month` | — | Target a specific month (YYYY-MM) regardless of manifest |
| `--limit-minutes` | `0` (no limit) | Stop after N minutes (for CI deadlines) |
| `--workers, -w` | `4` | Parallel workers (1–16) for page extraction |
| `--dry-run` | off | Verify without uploading |
| `--no-consolidate` | off | Skip the end-of-run annual consolidation |
| `--consolidate-start-year` | `2021` | First year to consider for consolidation |
| `--duckdb` | `:memory:` | Optional DuckDB file for debugging |
| `--no-curl` | off | Use httpx instead of system cURL |

Requires `IA_ACCESS_KEY` / `IA_SECRET_KEY` in the environment unless `--dry-run` is set.

### `verify` — Check IA coverage and detect gaps

```bash
baliza verify --start 2024-01-01 --end 2024-01-31
```

### `consolidate` — Rebuild annual archives manually

```bash
baliza consolidate --start-year 2021 [--force]
```

Usually not needed: `sync` already consolidates at the end of a successful run. Use this to force a rebuild of frozen past years (`--force`).

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
