// CATMAT/CATSER taxonomy resolver. Data is generated from the official gov.br
// xlsx snapshots by `scripts/fetch_catmat.py` — see that script for the source
// URL and refresh workflow. CATMAT is deduplicated to PDM level (~10K kinds of
// materials); CATSER includes all ~3K active services.

import rawEntries from './catmat-data.json' with { type: 'json' };

export interface CatmatEntry {
  code: string;
  description: string;
  type: 'CATMAT' | 'CATSER';
}

export const CATMAT_ENTRIES: readonly CatmatEntry[] = rawEntries as CatmatEntry[];

function normalize(raw: string): string {
  return raw
    .toLowerCase()
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .trim();
}

const NORMALIZED: ReadonlyArray<{ entry: CatmatEntry; desc: string; code: string }> =
  CATMAT_ENTRIES.map((entry) => ({
    entry,
    desc: normalize(entry.description),
    code: entry.code.toLowerCase(),
  }));

// PDM-level data is generic ("PAPEL PARA IMPRESSÃO FORMATADO"), so a SKU-level
// query ("papel sulfite branco A4 75g") rarely matches every word. Scoring by
// number-of-matched-words surfaces the closest catalog category for any query.
export function searchCatmat(query: string, limit = 50): CatmatEntry[] {
  const q = normalize(query);
  if (!q) return [];
  const words = q.split(/\s+/).filter((w) => w.length >= 2);
  if (!words.length) return [];

  type Scored = { entry: CatmatEntry; score: number; desc: string };
  const scored: Scored[] = [];
  for (const { entry, desc, code } of NORMALIZED) {
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
