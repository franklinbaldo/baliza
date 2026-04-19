import Papa from 'papaparse';
import { z } from 'zod';
import type { ArchivedTable } from './archive/schema';

export const IA_MANIFEST_URL =
  'https://archive.org/download/baliza-pncp-manifest/manifest.csv';

// Every row must provide all three fields; a manifest with malformed rows
// would otherwise corrupt downstream lookups (e.g. a `parquet_url` of the
// empty string was previously filtered silently). Invalid rows are dropped
// with a warning log so the archive layer degrades gracefully instead of
// failing outright.
const ManifestRowSchema = z.object({
  data_particao: z.string().min(1),
  table_name: z.string().min(1),
  parquet_url: z.string().min(1),
});

type ManifestRow = z.infer<typeof ManifestRowSchema>;

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
  const parsed = Papa.parse<unknown>(await res.text(), {
    header: true,
    skipEmptyLines: true,
  });
  const validated: ManifestRow[] = [];
  for (const raw of parsed.data ?? []) {
    const result = ManifestRowSchema.safeParse(raw);
    if (result.success) {
      validated.push(result.data);
    } else {
      console.warn('[manifest] row validation failed', {
        issues: result.error.issues,
      });
    }
  }
  return validated;
}

export async function getLatestParquetInfo(
  tableName: ArchivedTable = 'contratos',
): Promise<ParquetInfo | null> {
  const hit = cachedByTable.get(tableName);
  if (hit) return hit;

  const promise = (async () => {
    try {
      const rows = (await fetchManifestRows()).filter(
        (r) => r.table_name === tableName,
      );
      if (rows.length === 0) return null;
      rows.sort((a, b) => b.data_particao.localeCompare(a.data_particao));
      const top = rows[0];
      return { url: top.parquet_url, dataParticao: top.data_particao };
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
