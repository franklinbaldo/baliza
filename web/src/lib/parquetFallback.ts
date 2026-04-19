import { getDuckDB } from './duckdb';
import { getLatestParquetInfo } from './ia-manifest';
import {
  ARCHIVED_TABLES,
  type ArchivedContrato,
  type ArchivedTable,
  type ArchivedTableRowMap,
} from './archive/schema';

export type ArchiveFailureReason =
  | 'no_manifest'
  | 'duckdb_init_failed'
  | 'sql_error'
  | 'timeout'
  | 'empty';

export type ArchiveResult<T> =
  | { ok: true; rows: T[]; dataParticao: string | null }
  | { ok: false; reason: ArchiveFailureReason };

export interface QueryArchivedOpts {
  limit?: number;
  orderByColumn?: string;
  timeoutMs?: number;
}

const DEFAULT_QUERY_TIMEOUT_MS = 8_000;

// Matches a safe SQL identifier — letters, digits, underscores. Column names
// still flow through this check; table names are matched against a closed
// allow-list (ARCHIVED_TABLES) instead.
const SAFE_IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/;

function escapeSqlLiteral(value: string): string {
  return value.replace(/'/g, "''");
}

function isArchivedTable(name: string): name is ArchivedTable {
  return (ARCHIVED_TABLES as readonly string[]).includes(name);
}

export function archiveErrorMessage(reason: ArchiveFailureReason): string {
  switch (reason) {
    case 'no_manifest':
      return 'Arquivo histórico indisponível — manifesto do Internet Archive não pôde ser carregado.';
    case 'duckdb_init_failed':
      return 'Arquivo histórico indisponível — motor de consulta local não inicializou.';
    case 'sql_error':
      return 'Arquivo histórico indisponível — consulta ao parquet falhou.';
    case 'timeout':
      return 'Arquivo histórico indisponível — consulta ao parquet excedeu o tempo limite.';
    case 'empty':
      return 'PNCP indisponível e arquivo histórico sem registro para este identificador.';
  }
}

function logFallbackServed(
  table: ArchivedTable,
  column: string,
  rowCount: number,
): void {
  console.info('[archive] fallback served', { table, column, rowCount });
}

function logFallbackFailed(
  table: ArchivedTable,
  column: string,
  reason: ArchiveFailureReason,
): void {
  console.info('[archive] fallback failed', { table, column, reason });
}

// Warm the manifest lookup so that when a detail view's PNCP fetch fails,
// the archive layer can resolve the snapshot URL without paying a cold
// network roundtrip. DuckDB is intentionally NOT instantiated here — the
// WASM runtime is heavy (several MB + a Web Worker) and must only load when
// a query actually runs.
export function prefetchArchive(
  tableName: ArchivedTable = 'contratos',
): void {
  void getLatestParquetInfo(tableName).catch(() => null);
}

export async function queryArchivedTable<K extends ArchivedTable>(
  table: K,
  column: string,
  value: string,
  opts: QueryArchivedOpts = {},
): Promise<ArchiveResult<ArchivedTableRowMap[K]>> {
  if (!isArchivedTable(table)) {
    logFallbackFailed(table as ArchivedTable, column, 'sql_error');
    return { ok: false, reason: 'sql_error' };
  }
  if (!SAFE_IDENT.test(column)) {
    logFallbackFailed(table, column, 'sql_error');
    return { ok: false, reason: 'sql_error' };
  }
  const { limit = 10, orderByColumn, timeoutMs = DEFAULT_QUERY_TIMEOUT_MS } = opts;
  if (orderByColumn !== undefined && !SAFE_IDENT.test(orderByColumn)) {
    logFallbackFailed(table, column, 'sql_error');
    return { ok: false, reason: 'sql_error' };
  }

  const info = await getLatestParquetInfo(table);
  if (!info) {
    logFallbackFailed(table, column, 'no_manifest');
    return { ok: false, reason: 'no_manifest' };
  }

  let conn;
  try {
    ({ conn } = await getDuckDB());
  } catch {
    logFallbackFailed(table, column, 'duckdb_init_failed');
    return { ok: false, reason: 'duckdb_init_failed' };
  }

  const safeUrl = escapeSqlLiteral(info.url);
  const safeValue = escapeSqlLiteral(value);
  const safeLimit = Math.max(1, Math.min(200, Math.floor(limit)));
  const orderBy = orderByColumn ? ` ORDER BY ${orderByColumn} DESC NULLS LAST` : '';
  const sql = `SELECT * FROM read_parquet('${safeUrl}') WHERE ${column} = '${safeValue}'${orderBy} LIMIT ${safeLimit}`;

  let timedOut = false;
  let timeoutHandle: ReturnType<typeof setTimeout> | null = null;
  // Kick off the query first so we can tell DuckDB to cancel it if the
  // timer wins the race. Without cancelSent() the worker keeps the query
  // running against the shared singleton connection and can tie up later
  // archive lookups.
  const queryPromise = conn.query(sql);
  // Swallow late rejections caused by cancelSent() — otherwise the
  // rejection after the race has already resolved with '__timeout__'
  // surfaces as an unhandled promise rejection.
  queryPromise.catch(() => null);
  const timeoutPromise = new Promise<'__timeout__'>((resolve) => {
    timeoutHandle = setTimeout(() => {
      timedOut = true;
      void conn.cancelSent().catch(() => null);
      resolve('__timeout__');
    }, Math.max(0, timeoutMs));
  });

  try {
    const raced = await Promise.race([queryPromise, timeoutPromise]);
    if (raced === '__timeout__') {
      logFallbackFailed(table, column, 'timeout');
      return { ok: false, reason: 'timeout' };
    }
    const rows = raced.toArray().map((r: unknown) => {
      const anyRow = r as { toJSON?: () => unknown };
      return (typeof anyRow.toJSON === 'function' ? anyRow.toJSON() : r) as ArchivedTableRowMap[K];
    });
    if (rows.length === 0) {
      logFallbackFailed(table, column, 'empty');
      return { ok: false, reason: 'empty' };
    }
    logFallbackServed(table, column, rows.length);
    return { ok: true, rows, dataParticao: info.dataParticao };
  } catch {
    if (timedOut) {
      logFallbackFailed(table, column, 'timeout');
      return { ok: false, reason: 'timeout' };
    }
    logFallbackFailed(table, column, 'sql_error');
    return { ok: false, reason: 'sql_error' };
  } finally {
    if (timeoutHandle !== null) clearTimeout(timeoutHandle);
  }
}

export function queryParquetFallback<T = ArchivedContrato>(
  column: string,
  value: string,
  limit = 10,
  orderByColumn?: string,
): Promise<ArchiveResult<T>> {
  return queryArchivedTable('contratos', column, value, {
    limit,
    orderByColumn,
  }) as Promise<ArchiveResult<T>>;
}

export function queryArchivedOrgaos(
  column: string,
  value: string,
  opts: QueryArchivedOpts = {},
) {
  return queryArchivedTable('orgaos', column, value, opts);
}

export function queryArchivedUnidades(
  column: string,
  value: string,
  opts: QueryArchivedOpts = {},
) {
  return queryArchivedTable('unidades', column, value, opts);
}

export function queryArchivedFornecedores(
  column: string,
  value: string,
  opts: QueryArchivedOpts = {},
) {
  return queryArchivedTable('fornecedores', column, value, opts);
}
