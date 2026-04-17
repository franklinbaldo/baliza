export const IA_MANIFEST_URL =
  'https://archive.org/download/baliza-pncp-manifest/manifest.csv';

interface ManifestRow {
  data_particao: string;
  table_name: string;
  parquet_url: string;
}

function parseManifestCsv(csv: string): ManifestRow[] {
  const lines = csv.trim().split(/\r?\n/);
  if (lines.length < 2) return [];
  const header = lines[0].split(',');
  const idx = {
    data_particao: header.indexOf('data_particao'),
    table_name: header.indexOf('table_name'),
    parquet_url: header.indexOf('parquet_url'),
  };
  if (idx.data_particao < 0 || idx.parquet_url < 0) return [];
  return lines.slice(1).map((line) => {
    const cols = line.split(',');
    return {
      data_particao: cols[idx.data_particao] ?? '',
      table_name: cols[idx.table_name] ?? '',
      parquet_url: cols[idx.parquet_url] ?? '',
    };
  });
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
      const rows = parseManifestCsv(await res.text()).filter(
        (r) => r.table_name === 'contratos' && r.parquet_url,
      );
      if (rows.length === 0) return null;
      rows.sort((a, b) => b.data_particao.localeCompare(a.data_particao));
      return rows[0].parquet_url;
    } catch {
      return null;
    }
  })();
  const result = await cached;
  if (result === null) cached = null;
  return result;
}
