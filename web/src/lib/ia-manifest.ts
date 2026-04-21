import Papa from 'papaparse';
import { z } from 'zod';
import type { ArchivedTable } from './archive/schema';

export const IA_MANIFEST_URL =
  'https://archive.org/download/baliza-pncp-manifest/manifest.csv';

const ManifestRowSchema = z.object({
  data_particao: z.string().min(1),
  table_name: z.string().min(1),
  parquet_url: z.string().url(),
  // Manifest v2 columns — optional so v1 rows still validate. When present
  // they let the web layer pick the narrowest shard for a given filter and
  // expose integrity metadata (sha256) to researchers (Journey 5/6).
  file_type: z.string().optional(),
  uf_sigla: z.string().optional(),
  sha256: z.string().optional(),
  // Papa.parse fills missing columns with '' for v1 rows; coerce would turn
  // that into 0 and fail .positive(), dropping otherwise-valid rows.
  row_group_size: z.preprocess(
    (v) => (v === '' || v === null || v === undefined ? undefined : v),
    z.coerce.number().int().positive().optional(),
  ),
});

export type ManifestRow = z.infer<typeof ManifestRowSchema>;

export interface ParquetInfo {
  url: string;
  dataParticao: string | null;
  sha256?: string;
}

export interface ShardFilter {
  ufSigla?: string;
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

export async function fetchManifestRows(): Promise<ManifestRow[]> {
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
  const rows: ManifestRow[] = [];
  for (const raw of parsed.data ?? []) {
    const result = ManifestRowSchema.safeParse(raw);
    if (result.success) {
      rows.push(result.data);
    } else {
      console.warn('[ia-manifest] skipping malformed row', {
        issues: result.error.issues,
        row: raw,
      });
    }
  }
  return rows;
}

// Treat rows without an explicit file_type as canonical (backward compat with
// v1 manifests that lacked the column).
function isCanonicalRow(r: ManifestRow): boolean {
  return !r.file_type || r.file_type === 'monthly_canonical';
}

export async function getLatestParquetInfo(
  tableName: ArchivedTable = 'contratos',
): Promise<ParquetInfo | null> {
  const hit = cachedByTable.get(tableName);
  if (hit) return hit;

  const promise = (async () => {
    try {
      const rows = (await fetchManifestRows()).filter(
        (r) => r.table_name === tableName && isCanonicalRow(r),
      );
      if (rows.length === 0) return null;
      rows.sort((a, b) => b.data_particao.localeCompare(a.data_particao));
      const top = rows[0];
      return {
        url: top.parquet_url,
        dataParticao: top.data_particao,
        sha256: top.sha256,
      };
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

/**
 * Return the narrowest set of parquet URLs satisfying the filter.
 *
 * When a ``uf_sigla`` equality filter is supplied and per-UF shards
 * exist in the manifest, this returns only the shards for that UF
 * (typically 1 file, ~1/15th the bytes of the canonical file).
 * Otherwise falls back to the single canonical URL — callers should
 * treat the returned list as interchangeable inputs to
 * ``read_parquet([...])``.
 */
export async function getParquetShardsForFilter(
  filter: ShardFilter,
  tableName: ArchivedTable = 'contratos',
): Promise<ParquetInfo[]> {
  const rows = (await fetchManifestRows()).filter(
    (r) => r.table_name === tableName,
  );
  if (rows.length === 0) return [];

  if (filter.ufSigla) {
    const uf = filter.ufSigla.toUpperCase();
    const shards = rows.filter(
      (r) => r.file_type === 'monthly_uf' && r.uf_sigla?.toUpperCase() === uf,
    );
    if (shards.length > 0) {
      shards.sort((a, b) => b.data_particao.localeCompare(a.data_particao));
      const newest = shards[0].data_particao;
      // Fall back to canonical when the newest shard partition trails the
      // canonical snapshot — otherwise UF queries would silently serve
      // older data than non-UF queries on the same page load. Shards
      // record the covers_through canonical month at registration time so
      // string compare resolves month-level lag too (e.g. canonical
      // "2025-04" is newer than a shard built when only "2025-01" existed).
      // When a shard's partition is still year-only (fresh IA item, no
      // canonical rows yet), its length-4 string sorts below any "YYYY-MM"
      // so the comparison stays correct.
      const canonical = await getLatestParquetInfo(tableName);
      if (canonical?.dataParticao && canonical.dataParticao > newest) {
        return [canonical];
      }
      // Restrict to the newest partition so semantics match the canonical
      // path (which returns a single snapshot). Mixing partitions would
      // conflate snapshots while callers still surface only one dataParticao.
      return shards
        .filter((r) => r.data_particao === newest)
        .map((r) => ({
          url: r.parquet_url,
          dataParticao: r.data_particao,
          sha256: r.sha256,
        }));
    }
  }

  const canonical = await getLatestParquetInfo(tableName);
  return canonical ? [canonical] : [];
}
