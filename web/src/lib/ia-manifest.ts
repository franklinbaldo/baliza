import Papa from 'papaparse';

export const IA_MANIFEST_URL =
  'https://archive.org/download/baliza-pncp-manifest/manifest.csv';

interface ManifestRow {
  data_particao?: string;
  table_name?: string;
  parquet_url?: string;
}

let cached: Promise<string | null> | null = null;

export function resetIaManifestCache() {
  cached = null;
}

export async function getLatestParquetUrl(): Promise<string | null> {
  if (cached) return cached;
  cached = (async () => {
    try {
      const res = await fetch(IA_MANIFEST_URL);
      if (!res.ok) return null;
      const parsed = Papa.parse<ManifestRow>(await res.text(), {
        header: true,
        skipEmptyLines: true,
      });
      const rows = (parsed.data ?? []).filter(
        (r) => r.table_name === 'contratos' && r.parquet_url,
      );
      if (rows.length === 0) return null;
      rows.sort((a, b) =>
        (b.data_particao ?? '').localeCompare(a.data_particao ?? ''),
      );
      return rows[0].parquet_url ?? null;
    } catch {
      return null;
    }
  })();
  const result = await cached;
  if (result === null) cached = null;
  return result;
}
