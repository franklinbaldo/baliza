## Baliza — Product Vision

### What it is

Baliza is an open archive and analytical layer over the Brazilian National
Public Procurement Portal (PNCP). It extracts contract data daily, snapshots it
as Parquet onto the Internet Archive, and serves an in-browser SQL explorer
backed by DuckDB WASM. There is no central server at runtime: every page is
static HTML, every dataset is a public Internet Archive item, and the source
code is open. The stance is preservation first, analysis second.

### Guiding principle

> "Se o Baliza é útil para quem trabalha profissionalmente com licitação —
> fornecedor B2G que prospecta e comprador público que contrata — ele é
> automaticamente útil para jornalista, pesquisador e cidadão. Os profissionais
> são o caso exigente; os demais consomem subconjuntos das mesmas
> capabilities."
>
> — Franklin Baldo

*If Baliza is useful for the people who work with public procurement
professionally — the B2G supplier who prospects and the public buyer who
contracts — then it is automatically useful for journalists, researchers and
citizens. The professionals are the exigent case; everyone else consumes
subsets of the same capabilities.*

This is a prioritization rule, not a market segmentation. When two roadmap
items compete, the one that better serves the exigent professional case wins,
because the resulting capability cascades down to the lighter use cases.

### The seven journeys

Each journey has a persona, a verbatim central question, the capabilities it
needs, an honest current state, and a link to the matching feature file under
`web/features/journeys/`.

#### 1. B2G supplier prospecting

The vendor who already sells to the public sector and wants to find more
buyers.

*"How do I sell my product to the public sector? Who has bought what I sell,
for how much, in which modality, from whom?"*

Capabilities needed:

- Aggregated market page per object (top buyers, top suppliers, price ranges)
- Filter contracts by UF and modality with recomputed aggregates
- Supplier page by CNPJ with history, competitors and average ticket
- Export of result sets as CSV with named columns
- Accent- and stop-word-tolerant search with empty-state suggestions

Current state: partial. `SearchHero` resolves CNPJ jumps and `/orgao` shows
agency rollups, but there is no `/mercado/{objeto}` aggregation, no supplier
page, and no CSV export from search results.

Feature file: [`web/features/journeys/01_supplier_prospecting.feature`](web/features/journeys/01_supplier_prospecting.feature)

#### 2. Public buyer

The mayor, secretary, or pregoeiro who needs to make a purchase that survives
audit.

*"How do I make this purchase defensibly? Is there a vigent framework
agreement I can ride on? What is the evidenced reference price?"*

Capabilities needed:

- Browse vigent registered-price frameworks (atas) for an object
- Generate a price reference (min/avg/median/max/stdev) and export as a
  citable PDF
- Compare procurement practice across municipalities of similar size
- Resolve the right CATMAT/CATSER code from a natural-language description
- Inspect the legal basis (cited articles) used by peers in similar
  exemptions

Current state: not yet served. None of the above capabilities are implemented;
the agency page hints at history but has no buyer-side workflow. Highest-value
gap on the roadmap.

Feature file: [`web/features/journeys/02_public_buyer.feature`](web/features/journeys/02_public_buyer.feature)

#### 3. Investigative journalist

The reporter chasing a story who needs evidence, citability and reach.

*"Does this contract have a suspicious pattern? I need a citable permalink, a
back-link to PNCP, CSV/markdown export, and rollups by CNPJ and agency."*

Capabilities needed:

- Permanent URL per contract under `/contratacao?id=...`
- Outbound link to the original PNCP record on every detail page
- Search state preserved in the query string for sharable links
- Markdown export for direct paste into a CMS
- Agency rollup with top suppliers and time series

Current state: largely served. Permalinks, PNCP back-link and shareable
queries exist; CSV export exists in the explorer but not in search results;
markdown export and ready-made agency rollups are missing.

Feature file: [`web/features/journeys/03_investigative_journalist.feature`](web/features/journeys/03_investigative_journalist.feature)

#### 4. Informed citizen

The non-specialist who read a news headline and wants to verify it without
learning procurement jargon.

*"The news said hospital X received R$ Y — is it real? I need plain-language
search, an inline glossary, and humanized context."*

Capabilities needed:

- Search by hospital, school or agency name without knowing the CNPJ
- Tooltip or glossary link on every technical term ("dispensa",
  "inexigibilidade")
- Detail pages rendered in plain language, not just schema columns
- Data freshness visible at all times, not hidden behind a "status" tab
- Geographic context when available

Current state: partial. Free-text PNCP search works; freshness is shown on
`/status` but not next to results; plain-language rendering and a glossary
do not exist.

Feature file: [`web/features/journeys/04_informed_citizen.feature`](web/features/journeys/04_informed_citizen.feature)

#### 5. Academic researcher

The economist or political scientist who needs a reproducible dataset, not a
dashboard.

*"I need a reproducible dataset, a stable schema, an academic citation, a
changelog, and bulk download of the Parquet files."*

Capabilities needed:

- Documented table schemas reachable from the explorer
- Hash and snapshot date per Parquet file
- Schema changelog
- Reproducible URL-encoded queries in the explorer
- Suggested academic citation block

Current state: partial. The explorer runs SQL over remote Parquet, the
manifest CSV exists on Internet Archive, but the schema is not browsable
in-app, there is no changelog page, and no citation generator.

Feature file: [`web/features/journeys/05_academic_researcher.feature`](web/features/journeys/05_academic_researcher.feature)

#### 6. Developer integrator

The third-party developer who wants to consume Baliza's data inside their own
project.

*"I want to consume Baliza's data in my own project — a Parquet manifest with
URLs and hashes, examples in Python, R and JS, and embeddable views."*

Capabilities needed:

- A `/desenvolvedores` page that surfaces the manifest (URL, hash, date, size)
- Consumption examples in Python (pandas), R (arrow), and JS (DuckDB WASM)
- Stable, documented Internet Archive naming convention
- Embeddable explorer view via `?sql=...&embed=true`
- CORS allowing direct Parquet fetches from third-party domains

Current state: partial. The Internet Archive items exist with a stable name,
the explorer code already loads remote Parquet through `IA_URL` templating,
but there is no `/desenvolvedores` page, no consumption guide, and no embed
mode.

Feature file: [`web/features/journeys/06_developer_integrator.feature`](web/features/journeys/06_developer_integrator.feature)

#### 7. Auditor / watchdog

The recurring observer (NGO, control body, accountability journalist) who
needs to be told when something matches a saved pattern.

*"I want alerts (RSS, webhook, email) when a recurring anomaly shows up — a
new contract for CNPJ X, exemptions above value Y in state Z."*

Capabilities needed:

- Save the current query as a "watch" persisted in localStorage
- RSS feed at `/alertas/[slug].xml` publishing new matches
- Configurable webhook fired on new matches (stateless via GitHub Action)
- Diff view showing what changed since the last visit to a saved query
- Per-CNPJ and per-agency subscription entry points

Current state: not yet served. None of the alerting infrastructure exists.
The `AlertBanner` component is UI-only.

Feature file: [`web/features/journeys/07_auditor_watchdog.feature`](web/features/journeys/07_auditor_watchdog.feature)

### What Baliza is NOT (non-goals)

- Not a replacement for PNCP. It is an analytical and archival layer on top
  of it; the canonical source remains the federal portal.
- Not a real-time tender-tracking tool today. The focus is retrospective
  intelligence over data that has already been published.
- Not monetized. Data is free, code is open, infrastructure cost is borne by
  the author.
- Not a purchase recommender or a legality judge. Baliza presents data; the
  user decides.
- Not a substitute for legal counsel or procurement consultancy.

### Information architecture (today vs. planned)

| Route                       | Status   | Primary journeys served       |
|-----------------------------|----------|-------------------------------|
| `/`                         | Today    | 1, 3, 4                       |
| `/explorador`               | Today    | 3, 5, 6                       |
| `/status`                   | Today    | 4, 5                          |
| `/contratacao?id=`          | Today    | 3, 4                          |
| `/orgao?cnpj=`              | Today    | 1, 2, 3                       |
| `/municipio?ibge=`          | Today    | 4                             |
| `/sobre`                    | Today    | 4, 5                          |
| `/mercado/{objeto}`         | Planned  | 1, 2                          |
| `/fornecedor/{cnpj}`        | Planned  | 1, 3                          |
| `/desenvolvedores`          | Planned  | 6                             |
| `/glossario`                | Planned  | 4                             |
| `/alertas/{slug}.xml`       | Planned  | 7                             |
| `/schema` and `/citation`   | Planned  | 5                             |

### Capability roadmap

The roadmap is driven by the BDD suite, not by this document. Tag a scenario
correctly and the picture below stays honest.

- **Shipped**: every scenario tagged `@green` across `web/features/**.feature`.
  Snapshot the count with `npm run test:bdd:report`.
- **In progress**: every `@wip` scenario. These fail today but the
  implementation path is known.
- **Planned**: every `@planned` scenario. These are stubs that throw on
  purpose; they document intent before any line of code is written.

### How to evolve this vision

This document is not immutable. Vision changes are accepted via pull request,
provided the PR also touches at least one `.feature` under `web/features/` —
either adding a new scenario, promoting a scenario from `@planned` to `@wip`
to `@green`, or removing a scenario whose intent has been dropped. The
features are the executable specification; this file is the prose summary.
