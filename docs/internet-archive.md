# Baliza Internet Archive Collection

Every artifact Baliza publishes carries `subject:baliza-pncp` so researchers can discover the full collection without reading source code.

## Discovery

```bash
# List all Baliza IA items
ia search 'subject:baliza-pncp'

# Or via the IA web search:
# https://archive.org/search?query=subject%3Abaliza-pncp
```

## Items maintained

| Identifier | Purpose |
|---|---|
| `baliza-pncp-raw` | Raw JSON mirror — monthly ZIP per resource |
| `baliza-pncp-{YYYY-MM}` | Monthly canonical Parquet per resource |
| `baliza-pncp-consolidated` | Annual rollup Parquet + per-UF shards |
| `baliza-pncp-manifest` | `manifest.csv` — single source of truth |
| `baliza-pncp-feeds` | Curated RSS feeds for public-watch slugs |
| `baliza-pncp-dimensions` | Cumulative dimension files (orgaos, fornecedores) |

## Subject tag

The constant `BALIZA_COLLECTION_TAG = "baliza-pncp"` in
`src/baliza/ia_uploader.py` is injected into the `subject` list of
every metadata block at upload time.

## Backfilling existing items

Items uploaded before the tag was introduced (before 2026-05) can be
patched without re-uploading:

```bash
IA_ACCESS_KEY=... IA_SECRET_KEY=... uv run python scripts/backfill_ia_subjects.py
# Dry-run first:
uv run python scripts/backfill_ia_subjects.py --dry-run
```

The script is idempotent — already-tagged items are skipped.

## IA collection vs subject tag

Baliza lives in the `opensource_media` IA collection (the default).
Creating a dedicated `baliza-pncp` collection requires IA admin approval.
The subject-tag approach works without admin access and is sufficient for
researcher discovery.
