import {
  archiveErrorMessage,
  queryParquetFallback,
  type ArchiveResult,
} from './parquetFallback';
import type { ArchivedContrato } from './archive/schema';

export interface DetailQueryArchiveConfig {
  column: string;
  identifier: string;
  limit?: number;
  orderByColumn?: string;
}

export interface DetailQueryConfig<TData, TRow = ArchivedContrato> {
  queryKey: readonly unknown[];
  enabled: boolean;
  fetchLive: () => Promise<TData>;
  archive: DetailQueryArchiveConfig;
  buildFromArchive: (result: {
    rows: TRow[];
    dataParticao: string | null;
  }) => TData;
}

// Collapses the try-live-then-archive skeleton that every detail view used
// to repeat by hand. Callers own the view-specific mapping of live payload
// and archive rows; this factory only wires the PNCP → parquet fallback and
// surfaces `archiveErrorMessage(reason)` on a double failure.
export function createDetailQuery<TData, TRow = ArchivedContrato>(
  config: DetailQueryConfig<TData, TRow>,
) {
  return {
    queryKey: config.queryKey,
    enabled: config.enabled,
    queryFn: async (): Promise<TData> => {
      try {
        return await config.fetchLive();
      } catch (pncpErr) {
        const archived = (await queryParquetFallback<TRow>(
          config.archive.column,
          config.archive.identifier,
          config.archive.limit,
          config.archive.orderByColumn,
        )) as ArchiveResult<TRow>;
        if (!archived.ok) {
          throw new Error(archiveErrorMessage(archived.reason), { cause: pncpErr });
        }
        return config.buildFromArchive({
          rows: archived.rows,
          dataParticao: archived.dataParticao,
        });
      }
    },
  };
}
