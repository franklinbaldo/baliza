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
  | 'empty';

export type ArchiveResult<T> =
  | { ok: true; rows: T[]; dataParticao: string | null }
  | { ok: false; reason: ArchiveFailureReason };

export interface QueryArchivedOpts {
  limit?: number;
  orderByColumn?: string;
}

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
    case 'empty':
      return 'PNCP indisponível e arquivo histórico sem registro para este identificador.';
  }
}

function logFallback(
  table: ArchivedTable,
  column: string,
  outcome: 'ok' | ArchiveFailureReason,
): void {
  console.info('[archive] fallback engaged', { table, column, reason: outcome });
}

// Warm the manifest lookup and DuckDB runtime in parallel so that when a
// detail view's PNCP fetch fails, the fallback can resolve without eating
// the cold-start latency.
export function prefetchArchive(
  tableName: ArchivedTable = 'contratos',
): void {
  void getLatestParquetInfo(tableName).catch(() => null);
  void getDuckDB().catch(() => null);
}

export async function queryArchivedTable<K extends ArchivedTable>(
  table: K,
  column: string,
  value: string,
  opts: QueryArchivedOpts = {},
): Promise<ArchiveResult<ArchivedTableRowMap[K]>> {
  if (!isArchivedTable(table)) {
    logFallback(table as ArchivedTable, column, 'sql_error');
    return { ok: false, reason: 'sql_error' };
  }
  if (!SAFE_IDENT.test(column)) {
    logFallback(table, column, 'sql_error');
    return { ok: false, reason: 'sql_error' };
  }
  const { limit = 10, orderByColumn } = opts;
  if (orderByColumn !== undefined && !SAFE_IDENT.test(orderByColumn)) {
    logFallback(table, column, 'sql_error');
    return { ok: false, reason: 'sql_error' };
  }

  const info = await getLatestParquetInfo(table);
  if (!info) {
    logFallback(table, column, 'no_manifest');
    return { ok: false, reason: 'no_manifest' };
  }

  let conn;
  try {
    ({ conn } = await getDuckDB());
  } catch {
    logFallback(table, column, 'duckdb_init_failed');
    return { ok: false, reason: 'duckdb_init_failed' };
  }

  const safeUrl = escapeSqlLiteral(info.url);
  const safeValue = escapeSqlLiteral(value);
  const safeLimit = Math.max(1, Math.min(200, Math.floor(limit)));
  const orderBy = orderByColumn ? ` ORDER BY ${orderByColumn} DESC NULLS LAST` : '';
  const sql = `SELECT * FROM read_parquet('${safeUrl}') WHERE ${column} = '${safeValue}'${orderBy} LIMIT ${safeLimit}`;

  try {
    const res = await conn.query(sql);
    const rows = res.toArray().map((r: unknown) => {
      const anyRow = r as { toJSON?: () => unknown };
      return (typeof anyRow.toJSON === 'function' ? anyRow.toJSON() : r) as ArchivedTableRowMap[K];
    });
    if (rows.length === 0) {
      logFallback(table, column, 'empty');
      return { ok: false, reason: 'empty' };
    }
    logFallback(table, column, 'ok');
    return { ok: true, rows, dataParticao: info.dataParticao };
  } catch {
    logFallback(table, column, 'sql_error');
    return { ok: false, reason: 'sql_error' };
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
