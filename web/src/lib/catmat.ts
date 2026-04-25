// CATMAT/CATSER taxonomy resolver. Data is generated from the official gov.br
// xlsx snapshots by `scripts/fetch_catmat.py` and served as a static asset at
// /data/catmat.json (~165 KB gzipped, ~960 KB raw, kept out of the JS bundle
// to stay under the bundle-guard budget). CATMAT is deduplicated to PDM level
// (~10K kinds of materials); CATSER includes all ~3K active services.
//
// Lookup is a fetch-once, then-in-memory model: the first call kicks off the
// fetch, subsequent calls reuse the cached array. Failures surface as a
// rejected promise so the caller can render an error state.

import { resolve } from './baseUrl';

export interface CatmatEntry {
  code: string;
  description: string;
  type: 'CATMAT' | 'CATSER';
}

interface NormalizedEntry {
  entry: CatmatEntry;
  desc: string;
  code: string;
}

const CATMAT_DATA_URL = resolve('data/catmat.json');

let cachedEntries: ReadonlyArray<NormalizedEntry> | null = null;
let inflight: Promise<ReadonlyArray<NormalizedEntry>> | null = null;

function normalize(raw: string): string {
  return raw
    .toLowerCase()
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .trim();
}

async function loadEntries(): Promise<ReadonlyArray<NormalizedEntry>> {
  if (cachedEntries) return cachedEntries;
  if (inflight) return inflight;
  inflight = (async () => {
    const res = await fetch(CATMAT_DATA_URL);
    if (!res.ok) throw new Error(`CATMAT data fetch failed: ${res.status}`);
    const raw = (await res.json()) as CatmatEntry[];
    const normalized: NormalizedEntry[] = raw.map((entry) => ({
      entry,
      desc: normalize(entry.description),
      code: entry.code.toLowerCase(),
    }));
    cachedEntries = normalized;
    return normalized;
  })();
  try {
    return await inflight;
  } finally {
    inflight = null;
  }
}

// Test-only: lets BDD steps inject a fixed dataset and skip the fetch.
export function __setCatmatEntriesForTest(entries: CatmatEntry[]): void {
  cachedEntries = entries.map((entry) => ({
    entry,
    desc: normalize(entry.description),
    code: entry.code.toLowerCase(),
  }));
}

// PDM-level data is generic ("PAPEL PARA IMPRESSÃO FORMATADO"), so a SKU-level
// query ("papel sulfite branco A4 75g") rarely matches every word. Scoring by
// number-of-matched-words surfaces the closest catalog category for any query.
export async function searchCatmat(query: string, limit = 50): Promise<CatmatEntry[]> {
  const q = normalize(query);
  if (!q) return [];
  const words = q.split(/\s+/).filter((w) => w.length >= 2);
  if (!words.length) return [];

  const entries = await loadEntries();

  type Scored = { entry: CatmatEntry; score: number; desc: string };
  const scored: Scored[] = [];
  for (const { entry, desc, code } of entries) {
    if (code === q) {
      scored.push({ entry, score: 1000, desc });
      continue;
    }
    let matched = 0;
    for (const w of words) {
      if (desc.includes(w)) matched++;
    }
    if (matched > 0) scored.push({ entry, score: matched, desc });
  }

  scored.sort((a, b) => {
    if (a.score !== b.score) return b.score - a.score;
    return a.desc.length - b.desc.length;
  });

  return scored.slice(0, limit).map(({ entry }) => entry);
}
