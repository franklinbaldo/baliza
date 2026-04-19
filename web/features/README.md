## Baliza BDD suite

Two coexisting layers of `.feature` files describe what Baliza does, plus a
third Python layer for the extraction backend (out of scope for this README).

### Layers

- **Component features** — `web/features/*.feature`. One file per Svelte
  component or page. These describe local behavior (loading states, error
  banners, search-pattern detection, fallbacks). Step bodies live in
  `web/src/components/__steps__/<name>.steps.ts`.
- **Journey features** — `web/features/journeys/0N_*.feature`. One file per
  user journey from [`VISION.md`](../../VISION.md). These describe what a
  persona is trying to accomplish end-to-end. Most scenarios are red on
  purpose: each one is an executable backlog item. Step bodies live in
  `web/src/components/__steps__/journeys/0N_*.steps.ts`.
- **Backend features** — `tests/features/*.feature` (pytest-bdd). Out of
  scope here.

### How to run

All commands run from `web/`.

| Command                       | What it does                                    |
|-------------------------------|-------------------------------------------------|
| `npm run test:bdd`            | Full suite; `@wip` and `@planned` excluded by default so CI stays green |
| `npm run test:bdd:journeys`   | Only the seven journey features                 |
| `npm run test:bdd:green`      | Only `@green` scenarios                         |
| `npm run test:bdd:wip`        | Only `@wip` scenarios (expect failures)         |
| `npm run test:bdd:planned`    | Only `@planned` scenarios (expect stub throws)  |
| `npm run test:bdd:report`     | Markdown table with green/wip/planned per journey |

Tag filtering is driven by the standard `VITEST_INCLUDE_TAGS` and
`VITEST_EXCLUDE_TAGS` environment variables, plus the default
`excludeTags: ['planned', 'wip']` configured in `web/vitest.setup.ts`.
Setting `VITEST_INCLUDE_TAGS` drops matching tags from the default
exclusion, so `npm run test:bdd:wip` actually exercises `@wip` scenarios.

### Tag conventions

Every scenario carries:

1. A status tag — exactly one of `@green`, `@wip`, `@planned`.
2. A journey tag — `@journey1` … `@journey7`. Set at the feature level for
   journey files; set per-file at the feature level for component files.
3. A capability tag — `@search`, `@market`, `@supplier`, `@export`,
   `@permalink`, `@glossary`, `@plain-language`, `@freshness`, `@explorer`,
   `@schema`, `@changelog`, `@citation`, `@api`, `@manifest`, `@embed`,
   `@cors`, `@alerts`, `@rss`, `@webhook`, `@diff`, `@frameworks`,
   `@catmat`, `@price-reference`, `@agency-rollup`.

Status definitions:

- `@green` — implemented and proven by either this scenario or a referenced
  component scenario. CI must keep these green.
- `@wip` — known gap; the failing assertion documents the missing piece.
  CI surfaces these but does not block.
- `@planned` — no implementation path yet; step body throws
  `planned: <capability> not yet implemented`. Excluded from default runs.

### How to contribute

A pull request that touches [`VISION.md`](../../VISION.md) must also touch at
least one `.feature`. The opposite is also expected: a new capability lands
first as a `@planned` scenario in the matching journey file, graduates to
`@wip` once the implementation is started, and finally to `@green` when the
behavior is reachable from the UI and asserted in a component feature.

### Why scenarios may stay red

The journey suite is a roadmap, not a regression net. A red scenario is
information, not a bug. The reporter (`npm run test:bdd:report`) keeps the
ratio honest.
