# Baliza ↔ br/acc — stable consumption interface

This document is the contract Baliza commits to so that downstream graph
ingesters — specifically [`World-Open-Graph/br-acc`](https://github.com/World-Open-Graph/br-acc),
whose `etl/src/bracc_etl/pipelines/pncp.py` is currently marked `stale` in
`docs/source_registry_br_v1.csv` — can replace their manual JSON import with a
fully automated pull from Baliza's Internet Archive (IA) snapshots.

The interface is intentionally narrow: a single CSV manifest at a fixed URL,
plus Parquet files referenced from that manifest, with a documented schema and
integrity hashes. There is no API, no auth, no rate limit beyond IA's own.

## Endpoints

All endpoints are public Internet Archive items. No credentials required.

| Resource | URL | Purpose |
|---|---|---|
| Manifest CSV | `https://archive.org/download/baliza-pncp-manifest/manifest.csv` | Single source of truth: lists every Parquet file Baliza has published, with hashes and metadata. |
| Monthly Parquet | `https://archive.org/download/baliza-pncp-<YYYY-MM>/contratos-<YYYY-MM>.parquet` | Per-month canonical snapshot. Resolved via `parquet_url` in the manifest — do not template it manually. |
| Annual consolidated Parquet | `https://archive.org/download/baliza-pncp-consolidated/contratos-<YYYY>.parquet` | Single-file annual archive, sorted by `(cnpj_orgao, data_publicacao DESC, numero_controle_pncp)`. Recommended for bulk ingestion. |
| Raw JSON ZIPs (optional) | `https://archive.org/download/baliza-pncp-raw/raw-<YYYY-MM>.zip` | Verbatim PNCP API responses. Use only for forensic re-derivation; the Parquet files are the supported format. |

The `baliza-pncp-manifest` IA item is the only URL that must be hard-coded in
a downstream consumer. Everything else (Parquet URLs, partition list, hashes,
schema version) is read from the manifest.

## Manifest CSV schema

Columns are stable; new columns may be **appended** but existing ones are
never renamed or removed. Older rows may have blanks for v2/v3 columns —
treat missing values as defaults below.

| Column | Type | Description |
|---|---|---|
| `data_particao` | `string` | Partition value. `YYYY-MM` for monthly rows, `YYYY` for shards keyed at the year level. |
| `table_name` | `string` | Logical table. Today only `contratos` is published. |
| `row_count` | `int` | Rows in the Parquet file. |
| `quarantine_count` | `int` | Rows rejected during validation; available separately as `quarantine_url`. |
| `ia_item_id` | `string` | Internet Archive item containing the file. |
| `raw_zip_url` | `string` | URL to the raw JSON zip for the same partition (may be blank for consolidated rows). |
| `parquet_url` | `string` | **Primary field consumers should use.** Direct URL to the Parquet file. |
| `quarantine_url` | `string` | URL to the quarantine Parquet (optional). |
| `uploaded_at` | `ISO-8601` | Manifest write timestamp. Use for freshness comparisons. |
| `file_type` | `enum` | One of `monthly_canonical`, `monthly_uf`. Empty/missing means `monthly_canonical` (legacy v1 rows). |
| `uf_sigla` | `string` | UF (e.g. `SP`) when `file_type=monthly_uf`; blank otherwise. |
| `sort_key` | `string` | Logical sort applied during write — informational. |
| `row_group_size` | `int` | Parquet row-group size — informational, lets readers tune projection. |
| `bloom_filter_columns` | `string` | Pipe-separated list of bloom-filtered columns — informational. |
| `sha256` | `hex` | SHA-256 of the Parquet file. **Verify after download.** |
| `file_size_bytes` | `int` | Byte length of the Parquet file. |
| `mirror_uploaded_at` | `ISO-8601` | When the raw JSON ZIP was mirrored to IA (v3, two-phase pipeline). |
| `raw_zip_sha256` | `hex` | SHA-256 of the raw JSON ZIP (v3). |
| `raw_zip_size_bytes` | `int` | Byte length of the raw JSON ZIP (v3). |
| `parquet_uploaded_at` | `ISO-8601` | When the Parquet was uploaded to IA (v3). |
| `parquet_schema_version` | `string` | Schema version tag — bump signals a non-additive Parquet change. |

### `file_type` semantics — pick one path

- **For full-history ingestion**: read `file_type IN ('monthly_canonical', '')`.
  These are non-overlapping monthly partitions; reading all of them yields
  every contract exactly once.
- **For per-UF sharding**: read `file_type = 'monthly_uf'`. Each shard is a
  subset of a `monthly_canonical` row partitioned by `uf_sigla`. **Do not mix
  shard rows with canonical rows for the same `(table_name, data_particao)`
  pair — you will double-count.**
- **For bulk ingestion (recommended for br-acc)**: skip the manifest entirely
  for past years and pull `https://archive.org/download/baliza-pncp-consolidated/contratos-<YYYY>.parquet`.
  Years older than ~60 days past year-end are frozen and stable; only the
  current year needs manifest-driven updates.

## Parquet schema (`contratos`)

Every `contratos` Parquet file is a flattened, snake_case projection of
PNCP's `RecuperarContratoDTO`. The columns below are stable; additions are
allowed, removals/renames bump `parquet_schema_version`.

Full list lives in `src/baliza/extractor.py::_flatten_contrato`. Columns most
relevant to graph modeling:

| Column | Type | Notes |
|---|---|---|
| `numero_controle_pncp` | `string` | **Primary key.** Globally unique contract identifier in PNCP. |
| `numero_controle_pncp_compra` | `string` | FK to the parent `contratacao` (the bid that originated this contract). |
| `cnpj_orgao` | `string` (14) | CNPJ of the contracting public agency. **Use this for `Agency` nodes.** |
| `razao_social_orgao` | `string` | Agency display name. |
| `poder_id`, `esfera_id` | `string` | Branch (Executivo/Legislativo/Judiciário) and sphere (Federal/Estadual/Municipal). |
| `codigo_unidade`, `nome_unidade` | `string` | Sub-unit inside the agency. |
| `uf_sigla`, `uf_nome`, `municipio_nome`, `codigo_ibge` | `string` | Geographic context — `codigo_ibge` is the canonical municipality key. |
| `cnpj_orgao_subrogado`, `razao_social_orgao_subrogado` | `string` | Sub-rogated agency (when applicable). |
| `codigo_unidade_subrogada`, `nome_unidade_subrogada`, `uf_sigla_subrogada` | `string` | Sub-unit inside the sub-rogated agency (when applicable). |
| `ni_fornecedor` | `string` | Supplier identifier — CNPJ for `tipo_pessoa='J'`, CPF for `'F'`, foreign ID otherwise. **Use this for `Supplier` nodes.** |
| `tipo_pessoa` | `string` | `J` (legal entity), `F` (individual), or other. |
| `nome_razao_social_fornecedor` | `string` | Supplier display name. |
| `codigo_pais_fornecedor` | `string` | ISO country code; non-`BR` indicates a foreign supplier. |
| `ni_fornecedor_subcontratado`, `nome_fornecedor_subcontratado`, `tipo_pessoa_subcontratada` | `string` | Subcontractor (when present) — graph this as a separate edge. |
| `tipo_contrato_id`, `tipo_contrato_nome` | `int`, `string` | Contract typology. |
| `categoria_processo_id`, `categoria_processo_nome` | `int`, `string` | Process category. |
| `valor_inicial`, `valor_global`, `valor_acumulado`, `valor_parcela`, `numero_parcelas` | `decimal` | Money fields in BRL. `valor_global` is the canonical headline value. |
| `data_publicacao` | `date`/`timestamp` | Publication date (aliased from `dataPublicacaoPncp`; also emitted as `data_publicacao_pncp` for compatibility). |
| `data_assinatura`, `data_vigencia_inicio`, `data_vigencia_fim` | `date` | Signing and validity window. |
| `data_atualizacao`, `data_atualizacao_global` | `timestamp` | Last modification timestamps — record-level and global. |
| `objeto_contrato`, `informacao_complementar`, `processo` | `string` | Free-text description and procedure number. |

Bloom filters are pre-built on `cnpj_orgao | ni_fornecedor | codigo_ibge` —
predicate pushdown on those columns is cheap.

## Recommended consumption pattern (Python)

```python
import csv, hashlib, io, urllib.request
import duckdb

MANIFEST_URL = "https://archive.org/download/baliza-pncp-manifest/manifest.csv"

def manifest_rows():
    with urllib.request.urlopen(MANIFEST_URL) as r:
        yield from csv.DictReader(io.TextIOWrapper(r, encoding="utf-8"))

def canonical_contratos(year: int | None = None):
    for row in manifest_rows():
        if row["table_name"] != "contratos":
            continue
        if (row.get("file_type") or "monthly_canonical") != "monthly_canonical":
            continue  # skip per-UF shards to avoid double-counting
        if year and not row["data_particao"].startswith(str(year)):
            continue
        yield row

def verify(path: str, expected_sha256: str) -> None:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    if h.hexdigest() != expected_sha256:
        raise RuntimeError(f"sha256 mismatch for {path}")

# Stream straight into DuckDB without local download:
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
urls = [r["parquet_url"] for r in canonical_contratos(year=2025)]
con.execute(f"""
    CREATE TABLE contratos AS
    SELECT * FROM read_parquet({urls!r}, union_by_name=true)
""")
```

For bulk historical loads, prefer the annual consolidated URL — one HTTP
range-read instead of twelve:

```python
con.execute("""
    CREATE TABLE contratos_2024 AS
    SELECT * FROM read_parquet(
        'https://archive.org/download/baliza-pncp-consolidated/contratos-2024.parquet'
    )
""")
```

## Suggested graph mapping for br/acc

This maps directly onto br/acc's `Bid`/`Company` model and extends it without
reshaping existing nodes.

```
(Agency:Company {cnpj: cnpj_orgao})
    -[:CONTRATOU {data: data_assinatura, valor: valor_global}]->
        (Contract:Contract {pncp_id: numero_controle_pncp})
            <-[:FORNECEU]-
                (Supplier:Company {ni: ni_fornecedor, tipo: tipo_pessoa})

(Contract) -[:ORIGINOU_DE]-> (Bid:Bid {pncp_id: numero_controle_pncp_compra})
(Contract) -[:LOCALIZADO_EM]-> (Municipio {ibge: codigo_ibge})
(Contract) -[:SUBCONTRATOU]-> (Supplier {ni: ni_fornecedor_subcontratado})  // optional
```

`Bid.pncp_id` (`numero_controle_pncp_compra`) is the join key into the
existing `pncp.py` Bid nodes — the new ingester can populate Contract /
Supplier without touching the current Bid loader.

## Cadence and freshness guarantees

- **GitHub Actions cron**: `pncp-sync.yml` runs daily, walks backwards from
  the previous month to `2021-09-01`, and uploads any missing monthly
  Parquet to IA.
- **Backfill horizon**: PNCP `contratos` data starts on 2021-09-06; earlier
  dates are clamped to `2021-09`. There is ~4 years of history available
  today.
- **Frozen years**: a year is considered immutable 60 days after year-end
  (`GRACE_DAYS=60` in `consolidator.py`). Past consolidated files do not
  change once frozen — safe to cache forever, keyed by `sha256`.
- **Current year**: re-consolidated whenever a new monthly canonical upload
  arrives. Use `parquet_uploaded_at` (or `uploaded_at` for v1 rows) to
  invalidate caches.

## Versioning policy

- The manifest CSV evolves additively. New columns appended; existing column
  names and semantics frozen.
- The Parquet schema evolves additively within the same
  `parquet_schema_version`. A bump of `parquet_schema_version` signals a
  non-additive change (rename, type change, semantic change of an existing
  column) — consumers should pin the version they target.
- Breaking changes to either contract are announced via a release note in
  this repo and a deprecation row in the manifest carrying both old and new
  field shapes for at least one full backfill cycle.

## What this interface intentionally does NOT cover

- **Bids / contratações**: the upstream PNCP `contratacoes` endpoint is not
  yet mirrored. `numero_controle_pncp_compra` lets you stub `Bid` nodes from
  `Contract` rows; full bid attributes need to come from br/acc's existing
  `pncp.py` (or wait for a future Baliza extension).
- **Atas (registered-price agreements), itens, empenhos**: planned but not
  shipped. When added, they appear as new `table_name` values in the same
  manifest — discoverable, no hard-coded URLs to update.
- **Entity resolution across CNPJs**: out of scope. Baliza emits the raw
  `cnpj_orgao` / `ni_fornecedor` exactly as PNCP returns them; cross-source
  identity resolution (CEIS, RFB, TSE) is br/acc's responsibility.
- **Authenticated, rate-limited, or private endpoints**: there are none and
  there will be none. Everything is on Internet Archive, anonymously
  fetchable, and licensed for reuse under the same terms as the underlying
  PNCP data.

## Contact / change requests

Open an issue at <https://github.com/franklinbaldo/baliza/issues> tagged
`integration:br-acc`. Schema changes proposed by br/acc that benefit other
consumers (e.g. additional projected columns, new `table_name`s) are
welcome.
