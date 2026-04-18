import { getDuckDB } from './duckdb';
import { getLatestParquetInfo } from './ia-manifest';

export interface ParquetFallback<T> {
  rows: T[];
  dataParticao: string | null;
}

// Matches a safe SQL identifier — letters, digits, underscores. Prevents any
// caller-supplied column name from smuggling SQL via string interpolation.
const SAFE_IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/;

function escapeSqlLiteral(value: string): string {
  return value.replace(/'/g, "''");
}

export async function queryParquetFallback<T = Record<string, unknown>>(
  column: string,
  value: string,
  limit = 10,
): Promise<ParquetFallback<T> | null> {
  if (!SAFE_IDENT.test(column)) return null;
  const info = await getLatestParquetInfo();
  if (!info) return null;
  const safeUrl = escapeSqlLiteral(info.url);
  const safeValue = escapeSqlLiteral(value);
  const safeLimit = Math.max(1, Math.min(200, Math.floor(limit)));
  const sql = `SELECT * FROM read_parquet('${safeUrl}') WHERE ${column} = '${safeValue}' LIMIT ${safeLimit}`;
  try {
    const { conn } = await getDuckDB();
    const res = await conn.query(sql);
    const rows = res.toArray().map((r: unknown) => {
      const anyRow = r as { toJSON?: () => unknown };
      return (typeof anyRow.toJSON === 'function' ? anyRow.toJSON() : r) as T;
    });
    if (rows.length === 0) return null;
    return { rows, dataParticao: info.dataParticao };
  } catch {
    return null;
  }
}
