// Live-then-archive query factory used by every detail view. Each view passes
// in its own live fetcher, archive call, and row-to-view mapper; the factory
// ties them into the standard TanStack Query options object: try PNCP first,
// fall through to the parquet snapshot on any failure, surface a friendly
// archive error if both layers miss.

import { archiveErrorMessage, type ArchiveResult } from './parquetFallback';

export interface DetailQueryConfig<TData, TRow> {
  queryKey: readonly unknown[];
  enabled: boolean;
  fetchLive: () => Promise<TData>;
  fetchArchive: () => Promise<ArchiveResult<TRow>>;
  mapArchiveRow: (input: {
    rows: TRow[];
    dataParticao: string | null;
  }) => TData;
}

export function createDetailQuery<TData, TRow>(
  config: DetailQueryConfig<TData, TRow>,
) {
  return {
    queryKey: config.queryKey,
    enabled: config.enabled,
    queryFn: async (): Promise<TData> => {
      try {
        return await config.fetchLive();
      } catch (pncpErr) {
        const archived = await config.fetchArchive();
        if (!archived.ok) {
          throw new Error(archiveErrorMessage(archived.reason), {
            cause: pncpErr,
          });
        }
        return config.mapArchiveRow({
          rows: archived.rows,
          dataParticao: archived.dataParticao,
        });
      }
    },
  };
}
