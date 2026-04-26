#!/usr/bin/env node
// Guards against accidental bundle bloat on the browser entry payload.
//
// "Entry" here is defined as the sum of every JS file that Astro emits into
// `dist/_astro/` EXCLUDING the lazy-loaded DuckDB runtime (wasm + workers +
// duckdb-browser-*.js). Those are code-split on demand, so they do not count
// against the cold page-load budget.
//
// CI fails if the measured size grows past BUDGET_BYTES, which is pinned at
// the current size + ~20 %. Bump the constant deliberately when a feature
// genuinely justifies the growth.

import { readdirSync, statSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DIST_DIR = join(__dirname, '..', 'dist', '_astro');

// Current baseline: 342_000 bytes — measured size (~333 KB) plus headroom,
// with room absorbed by the SupplierDetailView shipped for Journey 1 @green
// (/fornecedor?cnpj= supplier prospecting page), the citizen-first
// homepage islands (CityHero + CityPulse + CityNavLink + shared
// cityContext.svelte rune) that shift the landing fold from project
// manifesto to "what is my city buying?", the BuscaView UF + modality
// filters with aggregate strip (Journey 1 "Filter contracts by UF and
// modality recomputes aggregates") plus the lazy-loaded accent lexicon
// behind the zero-result suggestion path ("Empty search suggests accent-
// tolerant alternatives"), the Curva & Concreto refresh + IA-derived
// /status page from PR #471 (RawArchiveStatus island + dual PNCP/Consulta
// boundary schemas in pncp.ts), and the homepage UX redesign (PR #496:
// hero omni-search, Navigation dropdown) plus its offline geocoding follow-
// up (findNearestMunicipality in geo.ts; the 270 KB centroids dataset is a
// static asset and does NOT count against this entry-payload budget), and
// the /status public-CNPJ coverage audit (CoverageReport island that
// pulls a static reference list and runs a DuckDB-WASM SELECT DISTINCT
// SUBSTR(cnpj_orgao, 1, 8) on the contratos parquet — adds ~6 KB). When
// an intentional feature increases the baseline, raise this constant in
// the same commit and note what landed.
const BUDGET_BYTES = 342_000;

function isClientEntryFile(name) {
  if (!name.endsWith('.js')) return false;
  if (name.startsWith('duckdb')) return false;
  return true;
}

let files;
try {
  files = readdirSync(DIST_DIR);
} catch (err) {
  console.error(`[bundle-guard] cannot read ${DIST_DIR}: ${err.message}`);
  console.error('[bundle-guard] run `astro build` before this script.');
  process.exit(2);
}

const entries = files
  .filter(isClientEntryFile)
  .map((name) => {
    const full = join(DIST_DIR, name);
    return { name, size: statSync(full).size };
  })
  .sort((a, b) => b.size - a.size);

const total = entries.reduce((acc, entry) => acc + entry.size, 0);

const fmt = (bytes) => `${(bytes / 1024).toFixed(1)} KB (${bytes} bytes)`;

console.log('[bundle-guard] client entry JS (excluding DuckDB runtime):');
for (const entry of entries) {
  console.log(`  ${fmt(entry.size).padEnd(26)} ${entry.name}`);
}
console.log(`[bundle-guard] total: ${fmt(total)}`);
console.log(`[bundle-guard] budget: ${fmt(BUDGET_BYTES)}`);

if (total > BUDGET_BYTES) {
  const over = total - BUDGET_BYTES;
  console.error(
    `[bundle-guard] FAIL — entry payload exceeds budget by ${fmt(over)}.`,
  );
  console.error(
    '[bundle-guard] investigate the largest chunks above, or if the growth is intentional, raise BUDGET_BYTES in scripts/check-bundle-size.mjs with a note.',
  );
  process.exit(1);
}

console.log('[bundle-guard] OK — entry payload within budget.');
