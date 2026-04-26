#!/usr/bin/env node
// Builds a compact centroids dataset for in-browser reverse geocoding.
//
// Source: https://github.com/kelvins/Municipios-Brasileiros (CC-BY 4.0).
// Output: web/public/data/ibge-centroids.json — served as a static asset,
// fetched lazily by CityHero when the user clicks "Usar minha localização".
// Schema is array-of-arrays for compactness:
//   [ibge, nome, uf_sigla, lat, lng]
// 5571 municipalities ≈ 250 KB raw / ~65 KB gzipped.

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT_PATH = path.join(__dirname, '..', 'public', 'data', 'ibge-centroids.json');
const SOURCE_URL =
  'https://raw.githubusercontent.com/kelvins/Municipios-Brasileiros/main/json/municipios.json';

// Codigo_uf (IBGE) → sigla. Stable for the whole 26 + DF set.
const UF_CODE_TO_SIGLA = {
  11: 'RO', 12: 'AC', 13: 'AM', 14: 'RR', 15: 'PA', 16: 'AP', 17: 'TO',
  21: 'MA', 22: 'PI', 23: 'CE', 24: 'RN', 25: 'PB', 26: 'PE', 27: 'AL',
  28: 'SE', 29: 'BA',
  31: 'MG', 32: 'ES', 33: 'RJ', 35: 'SP',
  41: 'PR', 42: 'SC', 43: 'RS',
  50: 'MS', 51: 'MT', 52: 'GO', 53: 'DF',
};

async function main() {
  if (fs.existsSync(OUT_PATH)) {
    // Idempotent: skip the network if a fresh-enough copy is already on disk.
    const ageMs = Date.now() - fs.statSync(OUT_PATH).mtimeMs;
    if (ageMs < 30 * 24 * 60 * 60 * 1000) {
      console.log(`[ibge-centroids] up-to-date (${Math.round(ageMs / 86400000)}d old); skipping fetch`);
      return;
    }
  }

  console.log(`[ibge-centroids] fetching ${SOURCE_URL}`);
  const res = await fetch(SOURCE_URL);
  if (!res.ok) {
    throw new Error(`fetch failed: ${res.status} ${res.statusText}`);
  }
  // Source ships with a UTF-8 BOM; strip it before parsing.
  const text = (await res.text()).replace(/^\uFEFF/, '');
  const raw = JSON.parse(text);
  if (!Array.isArray(raw) || raw.length === 0) {
    throw new Error('unexpected source shape: empty or non-array');
  }

  const compact = [];
  let skipped = 0;
  for (const m of raw) {
    const ibge = String(m.codigo_ibge ?? '').trim();
    const nome = String(m.nome ?? '').trim();
    const uf = UF_CODE_TO_SIGLA[m.codigo_uf];
    const lat = Number(m.latitude);
    const lng = Number(m.longitude);
    if (!/^\d{7}$/.test(ibge) || !nome || !uf || !Number.isFinite(lat) || !Number.isFinite(lng)) {
      skipped++;
      continue;
    }
    // 4 decimals ≈ 11 m precision — far below city-boundary granularity.
    compact.push([ibge, nome, uf, round4(lat), round4(lng)]);
  }

  if (compact.length < 5500) {
    throw new Error(`sanity check failed: only ${compact.length} valid municipalities (expected ~5570)`);
  }

  fs.mkdirSync(path.dirname(OUT_PATH), { recursive: true });
  // Compact JSON (no indentation) — this asset is loaded by browsers, not read by humans.
  fs.writeFileSync(OUT_PATH, JSON.stringify(compact));
  const size = fs.statSync(OUT_PATH).size;
  console.log(
    `[ibge-centroids] wrote ${compact.length} entries to ${path.relative(process.cwd(), OUT_PATH)} (${(size / 1024).toFixed(1)} KB)${skipped ? `, skipped ${skipped} malformed rows` : ''}`,
  );
}

function round4(n) {
  return Math.round(n * 10000) / 10000;
}

main().catch((err) => {
  console.error('[ibge-centroids] FAILED:', err);
  process.exit(1);
});
