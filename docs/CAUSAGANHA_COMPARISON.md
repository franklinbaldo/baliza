# Baliza and CausaGanha: Operational Pipeline Comparison

> Honest comparison between two sister projects by the same author, focused on
> their **data downloading and Internet Archive archiving pipelines**.
>
> Analytics features (ratings, AI, embeddings) are acknowledged as inspirational
> but out of scope for this document.
>
> Last updated: 2026-04-03

## 1. Project Overview

| | Baliza | CausaGanha |
|---|--------|------------|
| **Domain** | Brazilian public procurement (PNCP) | Brazilian judicial decisions (DJEN) |
| **Data source** | 1 API (`pncp.gov.br/api/consulta/v1`) | 91 court websites via DJEN proxy |
| **Collection unit** | JSON pages (500 records/page) | ZIP files (one per tribunal per day) |
| **Output** | Parquet (contratos, orgaos, unidades) | Parquet (9 normalized tables) |
| **Archive** | Internet Archive (S3 PUT) | Internet Archive (S3 PUT + `ia` CLI) |
| **CLI** | `baliza` (Typer) | `causaganha` + `djen-backup` (Typer) |
| **Repo** | [franklinbaldo/baliza](https://github.com/franklinbaldo/baliza) | [franklinbaldo/causaganha](https://github.com/franklinbaldo/causaganha) |

Both projects share a mission of **Brazilian data transparency** and permanent
preservation via Internet Archive.

---

## 2. Shared Technical Foundation

| Aspect | Both Projects |
|--------|---------------|
| Language | Python 3.11+ |
| CLI framework | Typer |
| Package manager | uv |
| HTTP client | httpx (synchronous) |
| Retry library | tenacity |
| Database | DuckDB |
| Data format | Apache Parquet (Zstd compression) |
| Arrow integration | PyArrow |
| Testing | pytest + pytest-bdd |
| Type checking | mypy (strict) |
| Linting | ruff |
| CI/CD | GitHub Actions (scheduled workflows) |
| Data distribution | Internet Archive |

---

## 3. Side-by-Side Pipeline Comparison

### 3.1 Data Collection

| Aspect | Baliza | CausaGanha |
|--------|--------|------------|
| **Concurrency model** | sync httpx + `ThreadPoolExecutor` | sync httpx + `ThreadPoolExecutor` |
| **Worker count** | 4 (optimal for 1 rate-limited API) | 20 (needed for 91 independent courts) |
| **Retry strategy** | tenacity: 5 attempts, exp backoff 1s-60s | tenacity + per-tribunal stall detection |
| **Retryable errors** | HTTP 429, 5xx, connection, timeout | Same categories |
| **Rate limiting** | API-imposed; 4 workers is the ceiling | Per-tribunal; 20 workers across 91 courts |
| **SSRF protection** | IP validation, DNS rebinding prevention | Proxy-based (DJEN proxy service) |
| **Response safety** | 10MB streaming size limit | ZIP file validation |
| **Page/batch size** | 500 records per API page | 1 ZIP per tribunal per day |

**Key insight**: Both projects use the same concurrency model (synchronous httpx
with thread pools). The difference in worker count reflects API constraints, not
architectural choice. Baliza's PNCP API rate-limits aggressively beyond 4
concurrent requests; causaganha targets 91 independent endpoints.

### 3.2 State Management

| Aspect | Baliza | CausaGanha |
|--------|--------|------------|
| **Checkpoint storage** | DuckDB tables (`baliza_state.*`) | JSON state files (`backfill-state.json`, `ia-state.json`) |
| **Checkpoint granularity** | Per-page (500-record resolution) | Per-tribunal cursor + per-date completion |
| **CI state persistence** | GitHub Actions artifacts (30-day retention) | GitHub Actions cache |
| **Coverage tracking** | `baliza_state.coverage` table with status field | `state/completed.txt` + `state/unavailable.txt` |
| **Stall detection** | Gap detection via `baliza verify` (reactive) | 60-day circuit breaker per tribunal (proactive) |
| **Corruption recovery** | Manual | Auto-detect + rebuild from scratch |

**Tradeoffs**: Baliza's DuckDB state is transactionally consistent and
queryable via SQL, which suits a single-API extractor well. CausaGanha's JSON
files are simpler to inspect and version but lack transactional guarantees --
appropriate when managing 91 independent sources where partial state loss is
recoverable.

### 3.3 Export and Archival

| Aspect | Baliza | CausaGanha |
|--------|--------|------------|
| **Export tables** | 3 (contratos, orgaos, unidades) | 9 normalized (comunicacoes, textos, destinatarios, partes, advogados, ...) |
| **Export memory** | O(1) streaming via `RecordBatchReader` | ThreadPoolExecutor for parallel table export |
| **Compression** | Zstd level 3, Parquet v2.6 | Zstd compression |
| **Partitioning** | `ano=YYYY/mes=MM/` (monthly exports) | Tribunal + year (`djen-{tribunal}-{year}`) |
| **IA upload method** | Custom S3 PUT script (`upload_internet_archive.py`) | Integrated into collection pipeline |
| **IA resumability** | HEAD check compares local vs remote file size | Checkpoint-based skip of already-uploaded items |
| **IA metadata** | Manifest-driven: row count, date range, SHA256, subjects | Checksums alongside files |
| **IA item naming** | `baliza-pncp-YYYY-MM-DD` (daily), `baliza-contratos-YYYY-MM` (monthly) | `djen-{tribunal}-{year}` |
| **Metadata file** | `_metadata.json` per day + `manifest.json` with checksums | Per-item metadata |

**Key insight**: Baliza has a more mature IA upload pipeline with richer metadata
(manifest-driven, SHA256 checksums, structured JSON summaries). CausaGanha
uploads during collection for speed but with less metadata enrichment.

### 3.4 GitHub Actions Orchestration

| Aspect | Baliza | CausaGanha |
|--------|--------|------------|
| **Collection frequency** | Every 30 minutes | Every 20 minutes |
| **Daily export** | 5 AM UTC | 6 AM UTC |
| **Timeout handling** | `timeout-minutes: 25` (abrupt GHA kill) | Per-run deadline: 17 minutes (graceful shutdown) |
| **Concurrency control** | Concurrency groups, `cancel-in-progress: false` | Same pattern |
| **Failure alerting** | Auto-creates GitHub Issue with job details | Telegram bot (zero-ZIPs alert, low-coverage alert) |
| **Backfill strategy** | Forward (oldest first), 5 days/batch | Backward (newest first), configurable |
| **Workflow count** | 4 (continuous, daily, backfill, monthly) | 2 primary (collect-today, collect-zips) + deploy |
| **Manual dispatch** | Date range, force flag | Date range, workers, tribunal filter, deadline, cache reset |

**Key insight**: CausaGanha's per-run deadline management is a significant
operational advantage. Instead of relying on GitHub Actions to kill the process
at 25 minutes (losing in-progress work), causaganha sets a 17-minute deadline
and gracefully checkpoints before the GHA timeout. This is the single most
impactful pattern to import.

### 3.5 Observability

| Aspect | Baliza | CausaGanha |
|--------|--------|------------|
| **Logging framework** | `logging` (stdlib) + `rich.Console` | `structlog` (structured JSON) |
| **Log format** | Human-readable text | JSON key-value pairs (machine-parseable) |
| **CI log parsing** | Grep through text output | Structured fields queryable by key |
| **Alerting channel** | GitHub Issues (on workflow failure) | Telegram (proactive: zero-ZIPs, low coverage) |
| **Health checks** | `baliza verify` (gap detection) | Coverage threshold checks (73/91 tribunals) |
| **Metrics in CI** | `GITHUB_STEP_SUMMARY` with row counts | Same + collection/upload statistics |

**Key insight**: structlog gives causaganha machine-parseable logs with bound
context (tribunal name, date, page number). Baliza mixes `logger.warning()`
format strings with `console.print()` Rich formatting. The structlog pattern is
a clear improvement for CI debugging and operational monitoring.

---

## 4. Operational Patterns Worth Importing

### Tier 1: High Impact, Low Risk

#### 1. Structured Logging (structlog)

**What**: Replace stdlib `logging` + `console.print()` mix with `structlog` for
JSON-structured, context-bound operational logging.

**Where in causaganha**: Used throughout; configured as a core dependency in `pyproject.toml`.

**Where in baliza**: `extractor.py` (line 38: `logger = logging.getLogger(__name__)`),
`daily_exporter.py`, `cli_simple.py`. Keep `rich.Console` only for user-facing
CLI output (panels, tables, progress bars).

**Effort**: 1 day | **Risk**: Low (drop-in replacement)

**Why it matters**: During 30-minute extraction runs in CI, structured logs with
bound context (`resource=contratos`, `date=2026-04-01`, `page=7/23`) make
debugging failures dramatically faster than grepping text output.

#### 2. Per-Run Deadline Management

**What**: Set an internal deadline (e.g., 22 minutes) that triggers graceful
checkpoint and shutdown before the GitHub Actions `timeout-minutes: 25` kills
the process abruptly.

**Where in causaganha**: `collect-zips.yml` uses a 17-minute deadline with the
`--deadline` CLI flag. The CLI monitors elapsed time and stops accepting new
work units when the deadline approaches.

**Where in baliza**: `extractor.py` `_extract_range_task()` (line 482+) and
`cli_simple.py` `extract` command. Add a `--deadline-minutes` CLI option that
propagates to the extractor loop.

**Effort**: 1 day | **Risk**: Low (additive, no behavior change without the flag)

**Why it matters**: Currently, if a GHA timeout kills baliza mid-page-fetch, the
checkpoint for that page is lost. With deadline management, extraction stops
cleanly at the last completed page.

#### 3. Stall / Circuit Breaker Detection

**What**: Detect when a PNCP resource consistently returns zero results and
surface this proactively, rather than silently continuing to poll.

**Where in causaganha**: 60-day consecutive absence detection per tribunal.
Tribunals that stop reporting are flagged as "stopped" and excluded from
collection until manually reset.

**Where in baliza**: Could be added to `extractor.py` and surfaced in `baliza
verify` / `baliza status`. Track consecutive zero-result days per resource in
`baliza_state` and warn when a threshold is hit (e.g., 7 days for PNCP, which
is more stable than 91 courts).

**Effort**: 1 day | **Risk**: Low

**Why it matters**: If PNCP changes its API or a resource stops returning data,
baliza currently runs extraction every 30 minutes indefinitely with zero results.
A circuit breaker would surface this as an actionable alert instead of silent
waste.

### Tier 2: Medium Impact, Medium Risk

#### 4. Richer GitHub Issue Alerting

**What**: Enhance the existing failure-reporting GitHub Issues with coverage
metrics, not just workflow failure details. Report zero-extraction days,
coverage drops, or prolonged gaps.

**Where in causaganha**: Smoke tests in `collect-zips.yml` check for zero ZIPs
collected and coverage below 73/91 tribunals, triggering alerts.

**Where in baliza**: Extend `daily-export.yml` and `historical-backfill.yml`
failure jobs to include coverage statistics from `baliza verify`.

**Effort**: 1 day | **Risk**: Low

#### 5. Multi-Phase Pipeline Separation

**What**: Explicitly separate the pipeline into collect -> transform -> export ->
upload stages, each independently resumable.

**Where in causaganha**: `scripts/pipeline/` has separate `collect.py`,
`consolidate.py`, `convert.py` scripts, each independently runnable and
checkpointed.

**Where in baliza**: Currently, extraction and DuckDB insertion are tightly
coupled in `extractor.py`. Export and upload are separate scripts. The main
benefit would be making the extract step write raw JSON to a staging area before
DuckDB insertion, enabling re-processing without re-downloading.

**Effort**: 3 days | **Risk**: Medium (architectural change)

#### 6. Per-Resource State Cursors

**What**: When baliza expands beyond `contratos` to support `atas-registro-preco`,
`termos-contrato`, etc., track state independently per resource with cursor-based
progress tracking.

**Where in causaganha**: Per-tribunal cursor tracking allows each of the 91
courts to progress independently. If one court stalls, others continue.

**Where in baliza**: `baliza_state.coverage` already supports per-resource
tracking via the `resource` column. The gap is cursor-based progress (like "last
successfully processed page for resource X on date Y") rather than
window-based status.

**Effort**: 2 days | **Risk**: Low (builds on existing schema)

### Tier 3: Evaluate Later

| Pattern | Notes |
|---------|-------|
| **Ibis for analytical queries** | Would clean up `verify` and `status` commands in `cli_simple.py`. Low urgency since current SQL works fine. |
| **GHA cache vs artifacts** | CausaGanha uses GHA cache (faster restore) vs baliza's artifacts. Worth benchmarking the restore time difference for `baliza.duckdb`. |
| **Backwards date processing** | CausaGanha backfills newest-first. Could be useful for baliza if recent data is more valuable. Add as a `--direction` flag to backfill. |
| **Increased collection frequency** | 20 min vs 30 min. Depends on PNCP API tolerance and whether more frequent runs yield meaningfully fresher data. |

---

## 5. Aspirational Only (Not Operational Patterns)

These causaganha features are **domain-specific or not pipeline patterns**.
They are interesting but not candidates for import into baliza's data pipeline:

| Feature | Why it's not operational |
|---------|------------------------|
| **Pydantic-AI analytics** | Judicial decision analysis; baliza is a data pipeline, not an analytics platform |
| **OpenSkill rating system** | Lawyer performance rating; domain-specific to legal analytics |
| **LanceDB vector embeddings** | Semantic search over judicial documents; no procurement use case |
| **Jules automation (23 AI personas)** | Development tooling, not pipeline architecture |
| **459+ BDD scenarios** | A scale metric, not a pattern. Baliza's tier system (`@tier0`-`@tier3`) already provides the infrastructure for test expansion. |
| **Ibis for write-heavy paths** | CausaGanha uses Ibis for consolidation queries. Baliza's write-heavy extraction path is already optimized with raw DuckDB SQL + Arrow batch insertion. |

---

## 6. Decision Log: What We Chose NOT to Import

| Pattern | Reason for rejection |
|---------|---------------------|
| **Async httpx migration** | PNCP is 1 API that rate-limits at ~4 concurrent requests. `ThreadPoolExecutor(4)` already saturates the API. Async adds complexity without throughput gain. CausaGanha also uses sync httpx -- the old comparison doc's claim of "asyncio everywhere" was inaccurate. |
| **JSON state files** | Baliza's DuckDB state tables are more robust for a single-API extractor: transactionally consistent, queryable via SQL, co-located with the data. JSON files make sense for causaganha's 91 independent sources where simplicity and inspectability matter more. |
| **`internetarchive` library** | Baliza's direct S3 PUT is more controllable and already has resumability via HEAD check. The `ia` library adds a heavyweight dependency for functionality we already have. |
| **20-worker parallelism** | PNCP would rate-limit us. 4 workers is the empirically optimal ceiling (benchmarked: 3.5s at 4 workers, 34s at 16 workers due to 429 errors). |
| **Telegram alerting** | GitHub Issues provide sufficient alerting for baliza's single-API pipeline. Telegram adds operational overhead (bot management, token rotation) without proportional benefit for a pipeline with 1 data source. |
| **Backwards backfill direction** | Baliza's forward (oldest-first) backfill ensures complete historical coverage. For PNCP procurement data, historical completeness matters more than recency of backfill. Can revisit if priorities change. |

---

## 7. Architectural Context

The projects differ because they serve fundamentally different data landscapes:

**CausaGanha** manages **91 independent, unreliable sources** (Brazilian courts).
Each tribunal has different availability patterns, formats, and failure modes.
This drives the need for per-source cursors, stall detection (60-day circuit
breaker), high worker counts (20), and frequent collection (every 20 min).
State lives in simple JSON files because per-tribunal state is independent and
can be rebuilt from Internet Archive if lost.

**Baliza** manages **1 well-structured API** (PNCP). The API is stable, has
consistent pagination, and rate-limits aggressively. This drives a simpler
architecture: fewer workers (4), DuckDB-based state (transactional
consistency), and longer collection intervals (30 min). The complexity lives in
data normalization (nested JSON to flat Parquet) and export quality (streaming
O(1) memory, rich manifests with checksums).

Neither approach is better -- they are appropriate responses to different
constraints. The patterns worth importing are the **operational practices**
(structured logging, deadline management, circuit breakers) that are valuable
regardless of data source count.
