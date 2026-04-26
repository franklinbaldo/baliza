import { getDuckDB } from './duckdb';
import { getLatestParquetInfo, getParquetShardsForFilter } from './ia-manifest';
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

export interface ArchiveWhereFilter {
  column: string;
  op: 'eq' | 'ilike' | 'gte' | 'lte';
  value: string;
}

async function runArchiveQuery<K extends ArchivedTable>(
  table: K,
  columnLabel: string,
  buildWhere: (safeValues: string[]) => string,
  values: string[],
  opts: QueryArchivedOpts,
  shardFilter?: { ufSigla?: string },
): Promise<ArchiveResult<ArchivedTableRowMap[K]>> {
  const { limit = 10, orderByColumn, timeoutMs = DEFAULT_QUERY_TIMEOUT_MS } = opts;
  if (orderByColumn !== undefined && !SAFE_IDENT.test(orderByColumn)) {
    logFallbackFailed(table, columnLabel, 'sql_error');
    return { ok: false, reason: 'sql_error' };
  }

  // Prefer per-UF shards when the filter includes a UF equality — cuts bytes
  // fetched roughly 15× for the dominant Brazilian states. Falls back to the
  // canonical monthly URL when no shards are published yet.
  const infos = shardFilter?.ufSigla
    ? await getParquetShardsForFilter(shardFilter, table)
    : await (async () => {
        const info = await getLatestParquetInfo(table);
        return info ? [info] : [];
      })();
  if (infos.length === 0) {
    logFallbackFailed(table, columnLabel, 'no_manifest');
    return { ok: false, reason: 'no_manifest' };
  }

  let conn;
  try {
    ({ conn } = await getDuckDB());
  } catch {
    logFallbackFailed(table, columnLabel, 'duckdb_init_failed');
    return { ok: false, reason: 'duckdb_init_failed' };
  }

  const urlList = infos
    .map((i) => `'${escapeSqlLiteral(i.url)}'`)
    .join(', ');
  const readClause =
    infos.length === 1
      ? `read_parquet(${urlList})`
      : `read_parquet([${urlList}])`;
  const safeValues = values.map(escapeSqlLiteral);
  const safeLimit = Math.max(1, Math.min(200, Math.floor(limit)));
  const orderBy = orderByColumn ? ` ORDER BY ${orderByColumn} DESC NULLS LAST` : '';
  const whereStr = buildWhere(safeValues);
  const sql = `SELECT * FROM ${readClause} WHERE ${whereStr}${orderBy} LIMIT ${safeLimit}`;

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
      logFallbackFailed(table, columnLabel, 'timeout');
      return { ok: false, reason: 'timeout' };
    }
    const rows = raced.toArray().map((r: unknown) => {
      const anyRow = r as { toJSON?: () => unknown };
      return (typeof anyRow.toJSON === 'function' ? anyRow.toJSON() : r) as ArchivedTableRowMap[K];
    });
    if (rows.length === 0) {
      logFallbackFailed(table, columnLabel, 'empty');
      return { ok: false, reason: 'empty' };
    }
    logFallbackServed(table, columnLabel, rows.length);
    return { ok: true, rows, dataParticao: infos[0].dataParticao };
  } catch {
    if (timedOut) {
      logFallbackFailed(table, columnLabel, 'timeout');
      return { ok: false, reason: 'timeout' };
    }
    logFallbackFailed(table, columnLabel, 'sql_error');
    return { ok: false, reason: 'sql_error' };
  } finally {
    if (timeoutHandle !== null) clearTimeout(timeoutHandle);
  }
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
  return runArchiveQuery(
    table,
    column,
    ([safeValue]) => `${column} = '${safeValue}'`,
    [value],
    opts,
  );
}

export async function queryArchivedTableWhere<K extends ArchivedTable>(
  table: K,
  filters: ArchiveWhereFilter[],
  opts: QueryArchivedOpts = {},
): Promise<ArchiveResult<ArchivedTableRowMap[K]>> {
  if (!isArchivedTable(table)) {
    logFallbackFailed(table as ArchivedTable, '*', 'sql_error');
    return { ok: false, reason: 'sql_error' };
  }
  if (filters.length === 0) {
    logFallbackFailed(table, '*', 'sql_error');
    return { ok: false, reason: 'sql_error' };
  }
  for (const f of filters) {
    if (!SAFE_IDENT.test(f.column)) {
      logFallbackFailed(table, f.column, 'sql_error');
      return { ok: false, reason: 'sql_error' };
    }
  }
  // Hint the resolver to pick per-UF shards when the query filters by
  // `uf_sigla = '...'`. Other ops (ILIKE, gte, lte) are too loose to map
  // to a single shard and keep using the canonical file.
  const ufFilter = filters.find(
    (f) => f.column === 'uf_sigla' && f.op === 'eq',
  );
  const shardFilter = ufFilter ? { ufSigla: ufFilter.value } : undefined;
  return runArchiveQuery(
    table,
    '*',
    (safeValues) =>
      filters
        .map(({ column, op }, i) => {
          const v = safeValues[i];
          switch (op) {
            case 'eq':    return `${column} = '${v}'`;
            case 'ilike': return `${column} ILIKE '%${v}%'`;
            case 'gte':   return `${column} >= '${v}'`;
            case 'lte':   return `${column} <= '${v}'`;
          }
        })
        .join(' AND '),
    filters.map((f) => f.value),
    opts,
    shardFilter,
  );
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

// ---------------------------------------------------------------------------
// City-level aggregates for the homepage WowStrip
// ---------------------------------------------------------------------------
// PNCP's live API has no GROUP BY; the homepage's "magnitude tiles" — total
// contracts, total estimated value, top buyer — would otherwise require
// fetching ~hundreds of rows from PNCP and aggregating client-side per
// city, which is slow and tied to PNCP availability. Querying the archived
// parquet via DuckDB-WASM is faster (sub-second after the first parquet
// shard load), covers the full historical range (years, not weeks), and
// has no upstream rate limit.

export interface CityArchiveAggregates {
  /** COUNT(*) of archived contratos for the city. */
  contratos: number;
  /** SUM(valor_global) treating NULLs as zero. */
  valorTotal: number;
  /** MAX(data_publicacao_pncp) — most recent publish timestamp seen. */
  ultimaPublicacao: string | null;
  /** MIN(data_particao) — earliest snapshot the city appears in. */
  primeiraParticao: string | null;
  /** MAX(data_particao) — most recent snapshot, used for IA attribution. */
  ultimaParticao: string | null;
  /** Most frequent contracting órgão for the city. */
  topOrgao: { cnpj: string; razaoSocial: string | null; n: number } | null;
}

interface AggregateRow {
  contratos: number | bigint;
  valor_total: number | null;
  ultima_publicacao: string | null;
  primeira_particao: string | null;
  ultima_particao: string | null;
}
interface TopOrgaoRow {
  cnpj_orgao: string;
  razao_social_orgao: string | null;
  n: number | bigint;
}

const CITY_AGGREGATES_TABLE: ArchivedTable = 'contratos';

export async function queryCityAggregates(
  ibge: string,
  opts: { timeoutMs?: number } = {},
): Promise<ArchiveResult<CityArchiveAggregates>> {
  // Defensive: only ever pass 7-digit IBGE codes through to SQL even though
  // upstream callers (cityState) already validate. The parquet column is a
  // string so a malformed value would silently match zero rows — better to
  // reject early.
  if (!/^\d{7}$/.test(ibge)) {
    logFallbackFailed(CITY_AGGREGATES_TABLE, 'codigo_ibge', 'sql_error');
    return { ok: false, reason: 'sql_error' };
  }

  // Reuse the shared shard-resolver. UF-sharded parquets aren't applicable
  // here (we filter by IBGE, not UF) so always hit the canonical info.
  const info = await getLatestParquetInfo(CITY_AGGREGATES_TABLE);
  if (!info) {
    logFallbackFailed(CITY_AGGREGATES_TABLE, 'codigo_ibge', 'no_manifest');
    return { ok: false, reason: 'no_manifest' };
  }

  let conn;
  try {
    ({ conn } = await getDuckDB());
  } catch {
    logFallbackFailed(CITY_AGGREGATES_TABLE, 'codigo_ibge', 'duckdb_init_failed');
    return { ok: false, reason: 'duckdb_init_failed' };
  }

  const safeIbge = escapeSqlLiteral(ibge);
  const readClause = `read_parquet('${escapeSqlLiteral(info.url)}')`;
  const totalsSql = `SELECT
      COUNT(*) AS contratos,
      COALESCE(SUM(valor_global), 0) AS valor_total,
      MAX(data_publicacao_pncp) AS ultima_publicacao,
      MIN(data_particao) AS primeira_particao,
      MAX(data_particao) AS ultima_particao
    FROM ${readClause}
    WHERE codigo_ibge = '${safeIbge}'`;
  const topOrgaoSql = `SELECT cnpj_orgao, razao_social_orgao, COUNT(*) AS n
    FROM ${readClause}
    WHERE codigo_ibge = '${safeIbge}'
    GROUP BY 1, 2
    ORDER BY n DESC
    LIMIT 1`;

  const timeoutMs = opts.timeoutMs ?? DEFAULT_QUERY_TIMEOUT_MS;
  let timedOut = false;
  let timeoutHandle: ReturnType<typeof setTimeout> | null = null;
  // Run the two queries in parallel — they're independent and DuckDB's
  // worker can pipeline them. Cancel on timeout via the shared cancelSent.
  const totalsPromise = conn.query(totalsSql);
  const topOrgaoPromise = conn.query(topOrgaoSql);
  totalsPromise.catch(() => null);
  topOrgaoPromise.catch(() => null);
  const timeoutPromise = new Promise<'__timeout__'>((resolve) => {
    timeoutHandle = setTimeout(() => {
      timedOut = true;
      void conn.cancelSent().catch(() => null);
      resolve('__timeout__');
    }, Math.max(0, timeoutMs));
  });

  try {
    const raced = await Promise.race([
      Promise.all([totalsPromise, topOrgaoPromise]),
      timeoutPromise,
    ]);
    if (raced === '__timeout__') {
      logFallbackFailed(CITY_AGGREGATES_TABLE, 'codigo_ibge', 'timeout');
      return { ok: false, reason: 'timeout' };
    }
    const [totalsResult, topOrgaoResult] = raced;
    const totalsRows = totalsResult.toArray().map((r: unknown) => {
      const anyRow = r as { toJSON?: () => unknown };
      return (typeof anyRow.toJSON === 'function' ? anyRow.toJSON() : r) as AggregateRow;
    });
    if (totalsRows.length === 0) {
      logFallbackFailed(CITY_AGGREGATES_TABLE, 'codigo_ibge', 'sql_error');
      return { ok: false, reason: 'sql_error' };
    }
    const totals = totalsRows[0];
    const topOrgaoRows = topOrgaoResult.toArray().map((r: unknown) => {
      const anyRow = r as { toJSON?: () => unknown };
      return (typeof anyRow.toJSON === 'function' ? anyRow.toJSON() : r) as TopOrgaoRow;
    });
    const topOrgaoRow = topOrgaoRows[0];

    const aggregates: CityArchiveAggregates = {
      contratos: Number(totals.contratos),
      valorTotal: Number(totals.valor_total ?? 0),
      ultimaPublicacao: totals.ultima_publicacao ?? null,
      primeiraParticao: totals.primeira_particao ?? null,
      ultimaParticao: totals.ultima_particao ?? null,
      topOrgao: topOrgaoRow
        ? {
            cnpj: topOrgaoRow.cnpj_orgao,
            razaoSocial: topOrgaoRow.razao_social_orgao,
            n: Number(topOrgaoRow.n),
          }
        : null,
    };
    logFallbackServed(CITY_AGGREGATES_TABLE, 'codigo_ibge', aggregates.contratos);
    return { ok: true, rows: [aggregates], dataParticao: info.dataParticao };
  } catch {
    if (timedOut) {
      logFallbackFailed(CITY_AGGREGATES_TABLE, 'codigo_ibge', 'timeout');
      return { ok: false, reason: 'timeout' };
    }
    logFallbackFailed(CITY_AGGREGATES_TABLE, 'codigo_ibge', 'sql_error');
    return { ok: false, reason: 'sql_error' };
  } finally {
    if (timeoutHandle !== null) clearTimeout(timeoutHandle);
  }
}
