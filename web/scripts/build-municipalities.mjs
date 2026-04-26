#!/usr/bin/env node
// Builds compact datasets for in-browser reverse geocoding:
//
// 1. web/public/data/ibge-centroids.json — 5571 municipalities × [ibge,
//    nome, uf, lat, lng] from Municipios-Brasileiros (CC-BY 4.0). ~270 KB
//    raw / ~96 KB gzipped. Used for nearest-neighbour lookup.
// 2. web/public/data/br-boundary.json — Brazil's national outline as a
//    compact MultiPolygon (outer + holes), extracted from Natural Earth
//    10m admin_0_countries (public domain). ~206 KB raw / ~53 KB gzipped.
//    Used by isInsideBrazil() to reject foreign coordinates before they
//    silently snap onto the nearest Brazilian centroid. 10m (vs. 50m) is
//    necessary because 50m's coastal simplification cuts ~5 km off the
//    Rio coast (Copacabana, Leblon, Centro fall outside the polygon).
//
// Both assets are served as static files (out of the JS bundle) and
// fetched lazily by CityHero when the user clicks "Usar minha localização".

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = path.join(__dirname, '..', 'public', 'data');
const CENTROIDS_PATH = path.join(DATA_DIR, 'ibge-centroids.json');
const BOUNDARY_PATH = path.join(DATA_DIR, 'br-boundary.json');
const SOURCE_URL =
  'https://raw.githubusercontent.com/kelvins/Municipios-Brasileiros/main/json/municipios.json';
const NE_BOUNDARY_URL =
  'https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_10m_admin_0_countries.geojson';

const FRESH_MS = 30 * 24 * 60 * 60 * 1000;

// Codigo_uf (IBGE) → sigla. Stable for the whole 26 + DF set.
const UF_CODE_TO_SIGLA = {
  11: 'RO', 12: 'AC', 13: 'AM', 14: 'RR', 15: 'PA', 16: 'AP', 17: 'TO',
  21: 'MA', 22: 'PI', 23: 'CE', 24: 'RN', 25: 'PB', 26: 'PE', 27: 'AL',
  28: 'SE', 29: 'BA',
  31: 'MG', 32: 'ES', 33: 'RJ', 35: 'SP',
  41: 'PR', 42: 'SC', 43: 'RS',
  50: 'MS', 51: 'MT', 52: 'GO', 53: 'DF',
};

async function buildCentroids() {
  const hasOnDisk = fs.existsSync(CENTROIDS_PATH);
  if (hasOnDisk) {
    // Idempotent: skip the network if a fresh-enough copy is already on disk.
    const ageMs = Date.now() - fs.statSync(CENTROIDS_PATH).mtimeMs;
    if (ageMs < FRESH_MS) {
      console.log(`[ibge-centroids] up-to-date (${Math.round(ageMs / 86400000)}d old); skipping fetch`);
      return;
    }
  }

  console.log(`[ibge-centroids] fetching ${SOURCE_URL}`);
  let raw;
  try {
    const res = await fetch(SOURCE_URL);
    if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
    // Source ships with a UTF-8 BOM; strip it before parsing.
    const text = (await res.text()).replace(/^\uFEFF/, '');
    raw = JSON.parse(text);
    if (!Array.isArray(raw) || raw.length === 0) {
      throw new Error('unexpected source shape: empty or non-array');
    }
  } catch (err) {
    // Best-effort refresh: if the network or upstream is unavailable but we
    // already have a checked-in dataset on disk, keep the build reproducible
    // (offline CI, restricted runners) instead of hard-failing. Only abort
    // when there's nothing to fall back on.
    if (hasOnDisk) {
      console.warn(`[ibge-centroids] refresh failed (${err.message}); keeping existing ${path.relative(process.cwd(), CENTROIDS_PATH)}`);
      return;
    }
    throw err;
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
    // Same fallback rationale: a malformed upstream payload shouldn't break
    // builds when a known-good dataset is already committed.
    if (hasOnDisk) {
      console.warn(`[ibge-centroids] upstream returned only ${compact.length} valid rows (expected ~5570); keeping existing file`);
      return;
    }
    throw new Error(`sanity check failed: only ${compact.length} valid municipalities (expected ~5570)`);
  }

  fs.mkdirSync(DATA_DIR, { recursive: true });
  // Compact JSON (no indentation) — this asset is loaded by browsers, not read by humans.
  fs.writeFileSync(CENTROIDS_PATH, JSON.stringify(compact));
  const size = fs.statSync(CENTROIDS_PATH).size;
  console.log(
    `[ibge-centroids] wrote ${compact.length} entries to ${path.relative(process.cwd(), CENTROIDS_PATH)} (${(size / 1024).toFixed(1)} KB)${skipped ? `, skipped ${skipped} malformed rows` : ''}`,
  );
}

async function buildBoundary() {
  const hasOnDisk = fs.existsSync(BOUNDARY_PATH);
  if (hasOnDisk) {
    const ageMs = Date.now() - fs.statSync(BOUNDARY_PATH).mtimeMs;
    if (ageMs < FRESH_MS) {
      console.log(`[br-boundary] up-to-date (${Math.round(ageMs / 86400000)}d old); skipping fetch`);
      return;
    }
  }

  console.log(`[br-boundary] fetching ${NE_BOUNDARY_URL}`);
  let geom;
  try {
    const res = await fetch(NE_BOUNDARY_URL);
    if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
    const data = await res.json();
    const br = (data.features ?? []).find((f) => f.properties?.ADMIN === 'Brazil');
    if (!br) throw new Error('Brazil feature not found in Natural Earth dataset');
    geom = br.geometry;
    if (geom.type !== 'MultiPolygon' && geom.type !== 'Polygon') {
      throw new Error(`unexpected geometry type: ${geom.type}`);
    }
  } catch (err) {
    if (hasOnDisk) {
      console.warn(`[br-boundary] refresh failed (${err.message}); keeping existing ${path.relative(process.cwd(), BOUNDARY_PATH)}`);
      return;
    }
    throw err;
  }

  // Normalise to MultiPolygon shape: [ [outerRing, hole1, ...], ... ]
  // where each ring is [[lng, lat], ...]. Round coords to 3 decimals
  // (~111 m) — well past Natural Earth 10m's intrinsic resolution
  // (~1 km) and saves ~25 % of the byte count after deduping the
  // adjacent identical points that rounding produces.
  const polys = geom.type === 'MultiPolygon' ? geom.coordinates : [geom.coordinates];
  const compact = polys.map((poly) =>
    poly.map((ring) => dedupeAdjacent(ring.map(([lng, lat]) => [round3(lng), round3(lat)]))),
  );
  const totalPoints = compact.reduce((sum, p) => sum + p.reduce((s, r) => s + r.length, 0), 0);

  fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(BOUNDARY_PATH, JSON.stringify(compact));
  const size = fs.statSync(BOUNDARY_PATH).size;
  console.log(
    `[br-boundary] wrote ${compact.length} polygons / ${totalPoints} points to ${path.relative(process.cwd(), BOUNDARY_PATH)} (${(size / 1024).toFixed(1)} KB)`,
  );
}

function round4(n) {
  return Math.round(n * 10000) / 10000;
}

function round3(n) {
  return Math.round(n * 1000) / 1000;
}

function dedupeAdjacent(ring) {
  if (ring.length === 0) return ring;
  const out = [ring[0]];
  for (let i = 1; i < ring.length; i++) {
    const prev = out[out.length - 1];
    if (ring[i][0] !== prev[0] || ring[i][1] !== prev[1]) out.push(ring[i]);
  }
  return out;
}

async function main() {
  await buildCentroids();
  await buildBoundary();
}

main().catch((err) => {
  console.error('[build-municipalities] FAILED:', err);
  process.exit(1);
});
