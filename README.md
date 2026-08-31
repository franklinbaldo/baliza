# Baliza

[![PNCP Sync](https://github.com/franklinbaldo/baliza/actions/workflows/pncp-sync.yml/badge.svg)](https://github.com/franklinbaldo/baliza/actions/workflows/pncp-sync.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

**Baliza** (Backup Aberto de Licitações Zelando pelo Acesso) preserva e organiza dados do Portal Nacional de Contratações Públicas (PNCP) para consulta pública e análise reproduzível.

**[Abrir o Baliza na web →](https://franklinbaldo.github.io/baliza/)**

O projeto combina três superfícies do mesmo acervo:

- **site público** — descoberta por município, status, busca, dashboards, comparação e vistas analíticas sobre contratações públicas;
- **pipeline de preservação** — coleta dados do PNCP, publica snapshots no Internet Archive e consolida arquivos para análise;
- **CLI** — permite sincronizar, verificar e consolidar o acervo de forma reproduzível.

## Product vision and user journeys

Baliza is evolving from a contratos-centered browser into a multi-resource
PNCP archive: PCA → Publicações → Atas → Contratos → Itens. The
architectural contract for each resource lives in
[`web/features/pncp-resource-atlas.feature`](web/features/pncp-resource-atlas.feature),
the strategic framing in [`VISION.md`](VISION.md), and the promotion
roadmap in [`docs/plans/pncp-resource-pipeline.md`](docs/plans/pncp-resource-pipeline.md).

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
baliza sync --start-date 2021-09-01
```

Walks backwards from the previous month to `--start-date`, skipping months already present on Internet Archive (source of truth is the remote `manifest.csv`). For each missing month it extracts from PNCP, uploads the monthly Parquet snapshot to IA, and — once the run finishes with new data — rebuilds the annual consolidated archives.

| Flag | Default | Description |
|------|---------|-------------|
| `--start-date` | `2021-09-01` | Oldest date to backfill to. `contratos` data starts on 2021-09-06, so earlier dates are clamped to the 2021-09 monthly partition. |
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
uv run pytest tests/
uv run ruff check src/ tests/ scripts/
uv run mypy src/baliza
```

CI runs the same three commands. `tests/integration/test_pncp_cassettes.py`
runs separately, in the `pncp-cassette-check` workflow with its own network
block.

The standalone utilities under [`scripts/`](scripts/README.md) — data builders
the web build consumes, live-API probes, and Internet Archive maintenance — are
documented there.

## License

MIT
