import { getDuckDB } from './duckdb';
import { getLatestParquetInfo } from './ia-manifest';
import type { ArchivedContrato } from './archive/schema';

export type ArchiveFailureReason =
  | 'no_manifest'
  | 'duckdb_init_failed'
  | 'sql_error'
  | 'empty';

export type ArchiveResult<T> =
  | { ok: true; rows: T[]; dataParticao: string | null }
  | { ok: false; reason: ArchiveFailureReason };

// Matches a safe SQL identifier — letters, digits, underscores. Prevents any
// caller-supplied column name from smuggling SQL via string interpolation.
const SAFE_IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/;

function escapeSqlLiteral(value: string): string {
  return value.replace(/'/g, "''");
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

export async function queryParquetFallback<T = ArchivedContrato>(
  column: string,
  value: string,
  limit = 10,
  orderByColumn?: string,
): Promise<ArchiveResult<T>> {
  if (!SAFE_IDENT.test(column)) return { ok: false, reason: 'sql_error' };
  if (orderByColumn !== undefined && !SAFE_IDENT.test(orderByColumn)) {
    return { ok: false, reason: 'sql_error' };
  }

  const info = await getLatestParquetInfo();
  if (!info) return { ok: false, reason: 'no_manifest' };

  let conn;
  try {
    ({ conn } = await getDuckDB());
  } catch {
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
      return (typeof anyRow.toJSON === 'function' ? anyRow.toJSON() : r) as T;
    });
    if (rows.length === 0) return { ok: false, reason: 'empty' };
    return { ok: true, rows, dataParticao: info.dataParticao };
  } catch {
    return { ok: false, reason: 'sql_error' };
  }
}
