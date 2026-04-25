import { z } from 'zod';

export const IA_RAW_IDENTIFIER = 'baliza-pncp-raw';
export const IA_RAW_METADATA_URL = `https://archive.org/metadata/${IA_RAW_IDENTIFIER}`;
export const IA_RAW_DOWNLOAD_URL = `https://archive.org/download/${IA_RAW_IDENTIFIER}`;

const InternetArchiveFileSchema = z.object({
  name: z.string().min(1),
  size: z.coerce.number().int().nonnegative().optional(),
  mtime: z.coerce.number().int().positive().optional(),
  source: z.string().optional(),
  format: z.string().optional(),
  md5: z.string().optional(),
  sha1: z.string().optional(),
});

const InternetArchiveMetadataSchema = z.object({
  files: z.array(InternetArchiveFileSchema).default([]),
  files_count: z.coerce.number().int().nonnegative().optional(),
  item_size: z.coerce.number().int().nonnegative().optional(),
  item_last_updated: z.coerce.number().int().positive().optional(),
  server: z.string().optional(),
  dir: z.string().optional(),
});

export type InternetArchiveFile = z.infer<typeof InternetArchiveFileSchema>;

const ContractsPageSchema = z.object({
  totalRegistros: z.coerce.number().int().nonnegative(),
  totalPaginas: z.coerce.number().int().nonnegative().optional(),
  numeroPagina: z.coerce.number().int().nonnegative().optional(),
}).passthrough();

export interface RawArchiveMonth {
  period: string;
  fileName: string;
  contractsPageUrl: string | null;
  contractsCount: number | null;
}

export interface RawArchiveStatus {
  filesCount: number;
  rawZipCount: number;
  metadataFileCount: number;
  totalBytes: number;
  rawBytes: number;
  contractsCount: number | null;
  contractsCountCoverage: number;
  firstPeriod: string | null;
  latestPeriod: string | null;
  latestRawFile: InternetArchiveFile | null;
  latestContractsCount: number | null;
  rawArchives: RawArchiveMonth[];
  lastUpdatedIso: string | null;
  downloadUrl: string;
  metadataUrl: string;
  server: string | null;
  directory: string | null;
}

const RAW_ZIP_RE = /^raw-(\d{4}-\d{2})\.zip$/;

function epochSecondsToIso(value: number | undefined): string | null {
  if (!value) return null;
  return new Date(value * 1000).toISOString();
}

export function buildContractsPageUrl(
  fileName: string,
  server: string | null | undefined,
  directory: string | null | undefined,
): string | null {
  if (!server || !directory) return null;
  const params = new URLSearchParams({
    archive: `${directory.replace(/\/$/, '')}/${fileName}`,
    file: 'contratos_p1.json',
  });
  return `https://${server}/view_archive.php?${params.toString()}`;
}

export function buildRawArchiveStatus(raw: unknown): RawArchiveStatus {
  const parsed = InternetArchiveMetadataSchema.parse(raw);
  const files = parsed.files;
  const rawFiles = files
    .map((file) => {
      const match = RAW_ZIP_RE.exec(file.name);
      return match ? { file, period: match[1] } : null;
    })
    .filter((entry): entry is { file: InternetArchiveFile; period: string } => entry !== null)
    .sort((a, b) => a.period.localeCompare(b.period));

  const latest = rawFiles.at(-1) ?? null;
  const newestMtime = Math.max(
    0,
    parsed.item_last_updated ?? 0,
    ...files.map((file) => file.mtime ?? 0),
  );

  const rawArchives = rawFiles.map((entry) => ({
    period: entry.period,
    fileName: entry.file.name,
    contractsPageUrl: buildContractsPageUrl(entry.file.name, parsed.server, parsed.dir),
    contractsCount: null,
  }));

  return {
    filesCount: parsed.files_count ?? files.length,
    rawZipCount: rawFiles.length,
    metadataFileCount: files.filter((file) => file.source === 'metadata' || file.format === 'Metadata').length,
    totalBytes: parsed.item_size ?? files.reduce((sum, file) => sum + (file.size ?? 0), 0),
    rawBytes: rawFiles.reduce((sum, entry) => sum + (entry.file.size ?? 0), 0),
    contractsCount: null,
    contractsCountCoverage: 0,
    firstPeriod: rawFiles[0]?.period ?? null,
    latestPeriod: latest?.period ?? null,
    latestRawFile: latest?.file ?? null,
    latestContractsCount: null,
    rawArchives,
    lastUpdatedIso: epochSecondsToIso(newestMtime || undefined),
    downloadUrl: IA_RAW_DOWNLOAD_URL,
    metadataUrl: IA_RAW_METADATA_URL,
    server: parsed.server ?? null,
    directory: parsed.dir ?? null,
  };
}

// Internet Archive mirrors occasionally stall mid-response. Without a timeout
// here the `/status` page would hang indefinitely waiting for the slowest
// view_archive.php endpoint instead of falling back to a partial view.
const CONTRACTS_PAGE_TIMEOUT_MS = 8_000;

async function fetchContractsCountFromPage(url: string): Promise<number | null> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), CONTRACTS_PAGE_TIMEOUT_MS);
  try {
    const res = await fetch(url, { signal: controller.signal });
    if (!res.ok) return null;
    return ContractsPageSchema.parse(await res.json()).totalRegistros;
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

async function mapWithConcurrency<T, R>(
  items: T[],
  limit: number,
  worker: (item: T) => Promise<R>,
): Promise<R[]> {
  const results = new Array<R>(items.length);
  let cursor = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (cursor < items.length) {
      const index = cursor;
      cursor += 1;
      results[index] = await worker(items[index]);
    }
  });
  await Promise.all(workers);
  return results;
}

async function addContractsCounts(status: RawArchiveStatus): Promise<RawArchiveStatus> {
  const rawArchives = await mapWithConcurrency(status.rawArchives, 4, async (archive) => ({
    ...archive,
    contractsCount: archive.contractsPageUrl
      ? await fetchContractsCountFromPage(archive.contractsPageUrl)
      : null,
  }));
  const knownCounts = rawArchives
    .map((archive) => archive.contractsCount)
    .filter((count): count is number => count !== null);
  const latest = rawArchives.at(-1) ?? null;

  return {
    ...status,
    rawArchives,
    contractsCount: knownCounts.length > 0
      ? knownCounts.reduce((sum, count) => sum + count, 0)
      : null,
    contractsCountCoverage: knownCounts.length,
    latestContractsCount: latest?.contractsCount ?? null,
  };
}

export async function fetchRawArchiveStatus(): Promise<RawArchiveStatus> {
  const res = await fetch(IA_RAW_METADATA_URL);
  if (!res.ok) {
    throw new Error(`Internet Archive respondeu ${res.status} ao consultar ${IA_RAW_IDENTIFIER}.`);
  }
  return addContractsCounts(buildRawArchiveStatus(await res.json()));
}
