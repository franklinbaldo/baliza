import {
  archiveErrorMessage,
  queryArchivedTableWhere,
  queryParquetFallback,
  type ArchiveResult,
  type ArchiveWhereFilter,
} from './parquetFallback';
import type { ArchivedContrato } from './archive/schema';

export interface ListQueryArchiveConfig {
  column: string;
  value: string;
  limit?: number;
  orderByColumn?: string;
  /**
   * Additional WHERE clauses applied at the SQL level — useful when the live
   * call narrows by something the simple `column = value` filter cannot
   * express (e.g. a modalidade scope). Pushing the filter into SQL avoids
   * the trap of post-filtering an already-LIMITed result set, which would
   * silently drop matches sitting just past the limit boundary.
   */
  extraFilters?: ArchiveWhereFilter[];
}

export interface ListQueryConfig<TData, TRow = ArchivedContrato> {
  queryKey: readonly unknown[];
  enabled: boolean;
  fetchLive: () => Promise<TData>;
  archive: ListQueryArchiveConfig;
  buildFromArchive: (result: {
    rows: TRow[];
    dataParticao: string | null;
  }) => TData;
}

// List-shaped counterpart to createDetailQuery. When the live call throws we
// fall back to a parquet snapshot query whose shape is: filter one column by
// equality, order by another column DESC, limit N. This matches what every
// list view in the app needs (latest N contracts for a município/órgão).
//
// We delegate to queryParquetFallback (which targets the contratos snapshot)
// so existing detail-view-fallback tests can keep mocking it as the single
// archive entry point.
export function createListQuery<TData, TRow = ArchivedContrato>(
  config: ListQueryConfig<TData, TRow>,
) {
  return {
    queryKey: config.queryKey,
    enabled: config.enabled,
    queryFn: async (): Promise<TData> => {
      try {
        return await config.fetchLive();
      } catch (pncpErr) {
        const archived: ArchiveResult<TRow> =
          config.archive.extraFilters && config.archive.extraFilters.length > 0
            ? ((await queryArchivedTableWhere(
                'contratos',
                [
                  {
                    column: config.archive.column,
                    op: 'eq',
                    value: config.archive.value,
                  },
                  ...config.archive.extraFilters,
                ],
                {
                  limit: config.archive.limit,
                  orderByColumn: config.archive.orderByColumn,
                },
              )) as ArchiveResult<TRow>)
            : await queryParquetFallback<TRow>(
                config.archive.column,
                config.archive.value,
                config.archive.limit,
                config.archive.orderByColumn,
              );
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
