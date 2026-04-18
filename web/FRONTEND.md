# Baliza Frontend Architecture Guide

This document describes the tech stack, idioms, and patterns for the Baliza web dashboard, adapted from the CausaGanha standard.
All frontend code lives under `web/`.

---

## Tech Stack Overview

| Layer | Technology |
|---|---|
| Meta-framework | Astro 5 |
| Component framework | Svelte 5 |
| Styling | Vanilla CSS with design tokens (No Tailwind) |
| Local state | Svelte 5 runes (`$state`, `$derived`) |
| Async state | TanStack Query (`@tanstack/svelte-query@^6`) |
| Build | Vite |
| Validation | Zod |
| Client-side parsing | PapaParse (for Manifest CSVs) |
| Testing | Vitest + Testing Library |
| Language | TypeScript (strict) |

---

## Deployment — GitHub Pages (SSG)

This project is deployed as a **100% static site**.
- **No server at runtime.** There are no API routes, no SSR, no middleware. Every page is a `.html` file generated at build time.
- **The primary data source is Internet Archive.** After the static page loads, Svelte islands fetch the live `manifest.csv` and parse it via PapaParse.

---

## Astro and Svelte Islands

Astro renders every component to static HTML at build time. A Svelte component only runs on the client if you add a `client:*` directive.

### When to Create `.astro` vs `.svelte` vs `lib/`

- **`.astro`**: Purely static output. Layouts, structural shells (Headers, Footers). Compositions of islands.
- **`.svelte`**: Highly interactive components that react to data changes (Heatmaps, Live Sync Tables).
- **`lib/`**: Framework-agnostic logic. Zod schemas, data fetching, Svelte stores.

### Rule of Data Hydration
Always fetch `manifest.csv` using a `QueryClient`. Do not hold complex state in Astro `<script>` tags. Pass control to a Svelte 5 component.

---

## Data Boundaries: Zod

Zod is strictly used to validate all external data at the boundary — especially the Internet Archive CSVs.

```ts
import { z } from 'zod';

const ManifestRowSchema = z.object({
  data_particao: z.string(),
  table_name: z.string(),
  row_count: z.number().or(z.string().transform(Number)),
  quarantine_count: z.number().or(z.string().transform(Number)),
});

type ManifestRow = z.infer<typeof ManifestRowSchema>;
// Parse every row using this schema to prevent UI silent failures
```

---

## Vanilla CSS & Design Tokens

Like CausaGanha, **Tailwind is strictly forbidden in new components.**
All components must use scoped `<style>` blocks referencing global CSS variables (`var(--color-primary)`, `var(--space-md)`) defined in `web/src/index.css`.

- **Do not** write inline `style="..."` with hardcoded values.
- **Do not** duplicate hex codes.

---

## PNCP API boundaries

Two distinct PNCP surfaces are consumed by the client:

- **Consulta API** (`https://pncp.gov.br/api/consulta/v1/...`) — the structured
  endpoints for agency / city / contract detail reads. These do **not** accept a
  free-text keyword parameter.
- **Portal search** (`https://pncp.gov.br/api/search?q=<term>&tipos_documento=edital&pagina=1`)
  — the keyword-backed index used by the public portal's own search bar; used
  by `SearchHero` for free-text queries and returns `{ items, total }`.

When any consulta read fails, detail views fall back to the latest Internet
Archive Parquet snapshot via `queryParquetFallback()`. Column identifiers are
validated against `^[A-Za-z_][A-Za-z0-9_]*$` and every literal is escaped with
the `'` → `''` pattern before it is embedded in the SQL string.
