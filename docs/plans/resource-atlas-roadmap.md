# Resource Atlas Roadmap

This roadmap tracks promotion of PNCP resources from
`PLANNED_RESOURCES` to `RESOURCES` in `src/baliza/resources/__init__.py`.
The architectural contract every phase must satisfy is captured in
[`web/features/pncp-resource-atlas.feature`](../../web/features/pncp-resource-atlas.feature)
and the strategic framing lives in [`VISION.md`](../../VISION.md).

The phases are ordered by leverage on the seven user journeys: open
opportunities (publicações) help the B2G supplier and the public buyer
*today*; PCA unlocks intent-vs-fact comparisons; item-level data unlocks
price intelligence; cross-resource analytics is the payoff.

## Phase 1 — Publicações as canonical archive resource

Goal: register `publicacoes` in `RESOURCES` so that
`baliza sync --resource publicacoes` mirrors and ingests open
opportunities monthly.

Blocking work, all tracked as TODO markers in
`src/baliza/resources/publicacoes.py`:

- Decide modalidade fan-out (`codigoModalidadeContratacao` is required
  by `/v1/contratacoes/publicacao`). Recommended: extend `FetchSpec`
  with an optional `required_params: dict[str, list[int|str]]` and have
  the extractor iterate the cartesian product per page request. Avoids
  registering one synthetic resource per modalidade.
- Implement `_flatten_publicacao` in `transforms.py` against
  `RecuperarCompraPublicacaoDTO`; mirror the snake_case shape of
  `_flatten_contrato` so the explorer and detail views can reuse the
  same column conventions.
- Confirm `numeroControlePNCP` is unique inside a single fetched window
  before committing to `current_state` dedup. Otherwise switch to
  `append_only` and add a `data_atualizacao_global DESC` ordering.
- Confirm `data_start` for the endpoint and set it on the resource;
  until then backfills walk months unnecessarily.

Exit criteria: characterization test under
`tests/characterization/` covering at least one mirrored month and the
flatten round-trip; `RESOURCES["publicacoes"]` exists; no contratos
regression.

## Phase 2 — PCA as canonical archive resource

Goal: register `pca` in `RESOURCES` and expose a `pca_itens` canonical
table.

Blocking work, all tracked as TODO markers in
`src/baliza/resources/pca.py`:

- Confirm which `/v1/pca/...` endpoint is the canonical snapshot
  source (atualizacao vs usuario vs an item-level alternative).
- Teach the mirror/uploader to handle `partition_strategy="annual"`.
  Today `mirror_month` and `_pending_mirror_months` are month-only.
  Minimum-viable change: add a `partition_period(spec)` helper that
  returns a `(start, end, label)` tuple and replace the month math in
  the two pending-loops with it.
- Implement an item-level flatten that emits one row per
  `PlanoContratacaoItemDTO` enriched with the parent plan's fields.
- Verify the composite PK `(id_pca_pncp, numero_item)`.

Exit criteria: same as Phase 1, plus the annual-partition primitive is
exercised by at least one passing year of PCA data.

## Phase 3 — Item-level canonical data

Goal: introduce `itens_contratos` (and later `itens_atas`,
`itens_publicacoes`) as derived canonical tables, enabling per-item
price comparability.

This phase is the first one that benefits from the
`derived_tables` slot already declared in `pncp_resources.PNCPResource`
but not yet used. The cleanest path is to hang derived tables off the
existing `CONTRATOS` and `ATAS` resources rather than minting a
top-level "itens" resource — items are not a PNCP endpoint of their own,
they are the long-form projection of contracts and atas payloads.

Blocking work:

- Confirm whether per-item rows are exposed in the public PNCP
  endpoints, or only via the per-contract detail endpoint. If the
  latter, this phase requires a per-key fan-out fetcher (one extra
  request per contract), with rate-limit budgeting.
- Define the canonical price-intelligence schema (CATMAT/CATSER code,
  unit, unit price, quantity, agency, supplier, date).

## Phase 4 — Cross-resource joins and analytics

Goal: surface PCA → publicações → atas → contratos → itens journeys
inside the explorer and as named DuckDB views. Examples:

- "Did this agency contract what its PCA promised?"  
  (PCA `objeto` ↔ contratos `objeto_contrato` joined on
  `cnpj_orgao` and time window.)
- "Open opportunities for an object that has historical price evidence."
  (publicações × itens.)
- "Atas vigentes whose underlying compra is now contratada."
  (atas × contratos via `numero_controle_pncp_compra`.)

Blocking work:

- Define the join keys formally as a small DAG in `transforms.py` so
  the frontend can list available cross-resource queries without
  hard-coding SQL.
- Add an explorer "Atlas" sidebar that lists tables grouped by their
  resource layer (intent → open → reusable → final → comparable).

## Out of scope for this roadmap

- Real-time tender tracking (still a non-goal in `VISION.md`).
- Per-user webhooks / email alerts (still requires a server).
- A separate canonical resource per modalidade.
