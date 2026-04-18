import Papa from 'papaparse';
import type { ArchivedTable } from './archive/schema';

export const IA_MANIFEST_URL =
  'https://archive.org/download/baliza-pncp-manifest/manifest.csv';

interface ManifestRow {
  data_particao?: string;
  table_name?: string;
  parquet_url?: string;
}

export interface ParquetInfo {
  url: string;
  dataParticao: string | null;
}

const cachedByTable = new Map<ArchivedTable, Promise<ParquetInfo | null>>();

export function resetIaManifestCache() {
  cachedByTable.clear();
}

const MANIFEST_RETRY_BACKOFF_MS = 500;

// One retry on network errors or 5xx. 4xx (including 404) is terminal and
// returns [] so the archive layer can surface a `no_manifest` reason.
async function fetchManifestOnce(): Promise<Response | null> {
  try {
    return await fetch(IA_MANIFEST_URL);
  } catch {
    return null;
  }
}

async function fetchManifestRows(): Promise<ManifestRow[]> {
  let res = await fetchManifestOnce();
  if (res === null || res.status >= 500) {
    await new Promise((r) => setTimeout(r, MANIFEST_RETRY_BACKOFF_MS));
    res = await fetchManifestOnce();
  }
  if (!res || !res.ok) return [];
  const parsed = Papa.parse<ManifestRow>(await res.text(), {
    header: true,
    skipEmptyLines: true,
  });
  return parsed.data ?? [];
}

export async function getLatestParquetInfo(
  tableName: ArchivedTable = 'contratos',
): Promise<ParquetInfo | null> {
  const hit = cachedByTable.get(tableName);
  if (hit) return hit;

  const promise = (async () => {
    try {
      const rows = (await fetchManifestRows()).filter(
        (r) => r.table_name === tableName && r.parquet_url,
      );
      if (rows.length === 0) return null;
      rows.sort((a, b) =>
        (b.data_particao ?? '').localeCompare(a.data_particao ?? ''),
      );
      const top = rows[0];
      return top.parquet_url
        ? { url: top.parquet_url, dataParticao: top.data_particao ?? null }
        : null;
    } catch {
      return null;
    }
  })();
  cachedByTable.set(tableName, promise);

  const result = await promise;
  if (result === null) cachedByTable.delete(tableName);
  return result;
}

export async function getLatestParquetUrl(
  tableName: ArchivedTable = 'contratos',
): Promise<string | null> {
  const info = await getLatestParquetInfo(tableName);
  return info?.url ?? null;
}
