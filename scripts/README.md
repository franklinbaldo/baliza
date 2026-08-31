# scripts/

One-file utilities that sit outside the `baliza` package: data builders the web
build consumes, probes and smoke tests against the live PNCP API, and one-off
migrations.

Nothing here is imported by `src/baliza/`. Anything not listed as one-off is
called by a GitHub Actions workflow or by the web build, so check the "Called
by" column before changing an output format.

Run them with `uv run python scripts/<name>.py`. Scripts carrying an inline
`# /// script` block declare their own dependencies and run under
`uv run scripts/<name>.py` without the project environment.

## Data builders

Regenerate JSON the web app reads at build time. Each one tolerates an upstream
outage by preserving the existing snapshot.

| Script | Output | Called by |
|--------|--------|-----------|
| `build_sync_stats.py` | `web/public/data/sync_stats.json` | `pncp-sync.yml`, `deploy.yml`, `visual-capture.yml`, web build |
| `build_ia_inventory.py` | `web/public/data/ia_inventory.json` | web build (`web/scripts/build-data.mjs`) |
| `build_frontend_config.py` | `web/src/lib/generated/frontend_exposures.ts` | `deploy.yml`, web build |
| `build_orgaos_publicos.py` | public-sector CNPJ reference dataset (Receita Federal dump) | `build-orgaos-publicos.yml` |
| `fetch_catmat.py` | CATMAT/CATSER catalog JSON for `web/src/lib/catmat.ts` | manual refresh |

## PNCP probes and smoke tests

Hit the live API, so they need network access and are not part of `pytest`.

| Script | Purpose | Called by |
|--------|---------|-----------|
| `probe_pca.py` | Pins endpoint, pagination and PK facts for `/v1/pca/*` | `pncp-probe.yml` |
| `probe_publicacoes.py` | Same, for `/v1/contratacoes/publicacao` | `pncp-probe.yml` |
| `smoke_pca.py` | End-to-end PCA fetch → validate → upsert | `pncp-probe.yml` |
| `smoke_publicacoes.py` | End-to-end modalidade fan-out → validate → upsert | `pncp-probe.yml` |
| `record_pncp_cassettes.py` | Re-records the VCR cassettes under `tests/cassettes/` | `pncp-cassette-check.yml` |

Refresh cassettes after a PNCP schema change:

```bash
uv run python scripts/record_pncp_cassettes.py
```

## Internet Archive maintenance

| Script | Purpose |
|--------|---------|
| `backfill_ia_subjects.py` | Adds the `baliza-pncp` subject tag to items uploaded before the tag existed. Run by `backfill-ia-subjects.yml`; see [docs/internet-archive.md](../docs/internet-archive.md). |
| `migrate_zips_to_single_item.py` | One-off: moves raw ZIPs from per-month items into `baliza-pncp-raw`. Supports `--dry-run`. |

Both need `IA_ACCESS_KEY` and `IA_SECRET_KEY`.

## One-off frontend migrations

Kept for reference; they rewrite files in place and are not wired to anything.

| Script | Purpose |
|--------|---------|
| `strip_styles.py` | Removes `<style>` blocks, inline styles and class attributes from HTML/Astro/Svelte files. |
| `extract_removed_classes.py` | Recovers the class names `strip_styles.py` deleted, by diffing against a git ref. |
