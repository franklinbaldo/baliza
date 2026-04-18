import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { vi, expect } from 'vitest';

// IMPORTANT: this file intentionally does NOT import ./shared. shared.ts
// globally mocks queryParquetFallback; here we exercise the REAL
// queryArchivedTable implementation and control duckdb + fetch directly.

const feature = await loadFeature('features/archive-fallback.feature');

interface DuckDBHandle {
  db: null;
  conn: { query: ReturnType<typeof vi.fn> };
}

let queryMock: ReturnType<typeof vi.fn>;
let duckdbInitCount = 0;
let duckdbInstance: DuckDBHandle | null = null;
let duckdbShouldThrow = false;

async function fakeGetDuckDB(): Promise<DuckDBHandle> {
  if (duckdbShouldThrow) throw new Error('duckdb boom');
  if (!duckdbInstance) {
    duckdbInitCount += 1;
    duckdbInstance = { db: null, conn: { query: queryMock } };
  }
  return duckdbInstance;
}

const getDuckDBSpy = vi.fn(fakeGetDuckDB);

vi.mock('../../lib/duckdb', () => ({
  getDuckDB: (...args: unknown[]) =>
    (getDuckDBSpy as unknown as (...a: unknown[]) => Promise<DuckDBHandle>)(...args),
}));

// NOTE: ia-manifest is NOT mocked. We mock the underlying global fetch so the
// real manifest module's per-table Promise cache is exercised (this is what
// makes prefetchArchive measurable — the second call hits the cache).

const PARQUET_URL_BY_TABLE: Record<string, string> = {
  contratos: 'https://archive.org/download/baliza-pncp-manifest/contratos.parquet',
  orgaos: 'https://archive.org/download/baliza-pncp-manifest/orgaos.parquet',
  unidades: 'https://archive.org/download/baliza-pncp-manifest/unidades.parquet',
  fornecedores: 'https://archive.org/download/baliza-pncp-manifest/fornecedores.parquet',
};

function manifestCsvAllTables(): string {
  const header = 'data_particao,table_name,parquet_url';
  const rows = Object.entries(PARQUET_URL_BY_TABLE).map(
    ([table, url]) => `2024-12-01,${table},${url}`,
  );
  return [header, ...rows].join('\n');
}

function installManifestFetch(status: number, body: string) {
  // Response bodies are single-use; hand out a fresh one per call so the
  // per-table manifest cache can issue multiple fetches against this stub.
  const fetchSpy = vi.fn().mockImplementation(() =>
    Promise.resolve(
      new Response(body, { status, headers: { 'Content-Type': 'text/csv' } }),
    ),
  );
  global.fetch = fetchSpy as unknown as typeof fetch;
  return fetchSpy;
}

async function resetAll() {
  const parquet = await import('../../lib/parquetFallback');
  void parquet; // keep module side-effects
  const manifest = await import('../../lib/ia-manifest');
  manifest.resetIaManifestCache();

  queryMock = vi.fn();
  duckdbInstance = null;
  duckdbInitCount = 0;
  duckdbShouldThrow = false;
  getDuckDBSpy.mockClear();
  getDuckDBSpy.mockImplementation(fakeGetDuckDB);
}

describeFeature(feature, ({ Scenario, BeforeEachScenario }) => {
  let result: Awaited<ReturnType<typeof callArchive>> | null = null;
  let fetchSpy: ReturnType<typeof vi.fn> | null = null;

  async function callArchive(
    table: 'contratos' | 'orgaos' | 'unidades' | 'fornecedores' | string,
    column: string,
    value = 'xyz',
    opts: { limit?: number; orderByColumn?: string } = {},
  ) {
    const { queryArchivedTable } = await import('../../lib/parquetFallback');
    return queryArchivedTable(
      table as 'contratos',
      column,
      value,
      opts,
    );
  }

  BeforeEachScenario(async () => {
    vi.restoreAllMocks();
    await resetAll();
    result = null;
    fetchSpy = null;
  });

  Scenario('Manifest endpoint 404 yields reason "no_manifest"', ({ Given, When, Then }) => {
    Given('the IA manifest endpoint returns 404', () => {
      fetchSpy = installManifestFetch(404, '');
    });
    When('queryArchivedTable is called for "contratos" filtered by "cnpj_orgao"', async () => {
      result = await callArchive('contratos', 'cnpj_orgao');
    });
    Then('the result should be a failure with reason "no_manifest"', () => {
      expect(result).toEqual({ ok: false, reason: 'no_manifest' });
      expect(fetchSpy).toHaveBeenCalled();
    });
  });

  Scenario('DuckDB init failure yields reason "duckdb_init_failed"', ({ Given, And, When, Then }) => {
    Given('the IA manifest resolves to a parquet url', () => {
      installManifestFetch(200, manifestCsvAllTables());
    });
    And('DuckDB initialization throws', () => {
      duckdbShouldThrow = true;
    });
    When('queryArchivedTable is called for "contratos" filtered by "cnpj_orgao"', async () => {
      result = await callArchive('contratos', 'cnpj_orgao');
    });
    Then('the result should be a failure with reason "duckdb_init_failed"', () => {
      expect(result).toEqual({ ok: false, reason: 'duckdb_init_failed' });
    });
  });

  Scenario('SQL returning zero rows yields reason "empty"', ({ Given, And, When, Then }) => {
    Given('the IA manifest resolves to a parquet url', () => {
      installManifestFetch(200, manifestCsvAllTables());
    });
    And('DuckDB returns no rows', () => {
      queryMock.mockResolvedValue({ toArray: () => [] });
    });
    When('queryArchivedTable is called for "contratos" filtered by "cnpj_orgao"', async () => {
      result = await callArchive('contratos', 'cnpj_orgao');
    });
    Then('the result should be a failure with reason "empty"', () => {
      expect(result).toEqual({ ok: false, reason: 'empty' });
    });
  });

  Scenario('SQL thrown error yields reason "sql_error"', ({ Given, And, When, Then }) => {
    Given('the IA manifest resolves to a parquet url', () => {
      installManifestFetch(200, manifestCsvAllTables());
    });
    And('DuckDB query throws', () => {
      queryMock.mockRejectedValue(new Error('syntax error'));
    });
    When('queryArchivedTable is called for "contratos" filtered by "cnpj_orgao"', async () => {
      result = await callArchive('contratos', 'cnpj_orgao');
    });
    Then('the result should be a failure with reason "sql_error"', () => {
      expect(result).toEqual({ ok: false, reason: 'sql_error' });
    });
  });

  Scenario('SQL exceeding the timeout yields reason "timeout"', ({ Given, And, When, Then }) => {
    Given('the IA manifest resolves to a parquet url', () => {
      installManifestFetch(200, manifestCsvAllTables());
    });
    And('DuckDB query never resolves', () => {
      queryMock.mockImplementation(() => new Promise(() => {}));
    });
    When('queryArchivedTable is called for "contratos" with timeoutMs 10', async () => {
      result = await callArchive('contratos', 'cnpj_orgao', 'xyz', { timeoutMs: 10 });
    });
    Then('the result should be a failure with reason "timeout"', () => {
      expect(result).toEqual({ ok: false, reason: 'timeout' });
    });
  });

  Scenario('Limit above 200 is clamped to 200', ({ Given, And, When, Then }) => {
    Given('the IA manifest resolves to a parquet url', () => {
      installManifestFetch(200, manifestCsvAllTables());
    });
    And('DuckDB captures the query text and returns one row', () => {
      queryMock.mockResolvedValue({
        toArray: () => [{ toJSON: () => ({ numero_controle_pncp: 'x' }) }],
      });
    });
    When('queryArchivedTable is called for "contratos" with limit 5000', async () => {
      result = await callArchive('contratos', 'cnpj_orgao', 'xyz', { limit: 5000 });
    });
    Then('the executed SQL should contain "LIMIT 200"', () => {
      expect(queryMock).toHaveBeenCalledTimes(1);
      const sql = queryMock.mock.calls[0][0] as string;
      expect(sql).toContain('LIMIT 200');
      expect(sql).not.toContain('LIMIT 5000');
      expect(result?.ok).toBe(true);
    });
  });

  Scenario('Unknown table is rejected without touching the manifest', ({ Given, When, Then }) => {
    Given('the IA manifest endpoint is never called', () => {
      fetchSpy = installManifestFetch(500, 'should not be reached');
    });
    When('queryArchivedTable is called for "not_a_table" filtered by "cnpj_orgao"', async () => {
      result = await callArchive('not_a_table', 'cnpj_orgao');
    });
    Then('the result should be a failure with reason "sql_error"', () => {
      expect(result).toEqual({ ok: false, reason: 'sql_error' });
      expect(fetchSpy).not.toHaveBeenCalled();
      expect(getDuckDBSpy).not.toHaveBeenCalled();
    });
  });

  Scenario('prefetchArchive warms manifest and DuckDB before a query runs', ({ Given, When, Then, And }) => {
    Given('prefetchArchive was called for "contratos"', async () => {
      fetchSpy = installManifestFetch(200, manifestCsvAllTables());
      queryMock.mockResolvedValue({
        toArray: () => [{ toJSON: () => ({ numero_controle_pncp: 'x' }) }],
      });
      const { prefetchArchive } = await import('../../lib/parquetFallback');
      prefetchArchive('contratos');
      // Let the fire-and-forget promises settle before the "later" query runs.
      await new Promise((r) => setTimeout(r, 0));
      await new Promise((r) => setTimeout(r, 0));
    });
    When('the caller later invokes queryArchivedTable for "contratos" filtered by "cnpj_orgao"', async () => {
      result = await callArchive('contratos', 'cnpj_orgao');
    });
    Then('the IA manifest should have been fetched exactly once', () => {
      expect(fetchSpy).toHaveBeenCalledTimes(1);
      expect(result?.ok).toBe(true);
    });
    And('DuckDB should have been initialized exactly once', () => {
      expect(duckdbInitCount).toBe(1);
    });
  });

  Scenario('Multi-table wrappers route through the table allow-list', ({ When, And, Then }) => {
    const calls: Array<{ table: string; sql: string }> = [];

    When('queryArchivedOrgaos is called', async () => {
      fetchSpy = installManifestFetch(200, manifestCsvAllTables());
      queryMock.mockImplementation(async (sql: string) => {
        calls.push({ table: detectTable(sql), sql });
        return { toArray: () => [{ toJSON: () => ({ cnpj: '00' }) }] };
      });
      const { queryArchivedOrgaos } = await import('../../lib/parquetFallback');
      await queryArchivedOrgaos('cnpj', '00000000000191');
    });
    And('queryArchivedUnidades is called', async () => {
      const { queryArchivedUnidades } = await import('../../lib/parquetFallback');
      await queryArchivedUnidades('codigo_unidade', '1');
    });
    And('queryArchivedFornecedores is called', async () => {
      const { queryArchivedFornecedores } = await import('../../lib/parquetFallback');
      await queryArchivedFornecedores('ni_fornecedor', '00000000000191');
    });
    Then('each call should request its own parquet snapshot from the manifest', () => {
      expect(calls).toHaveLength(3);
      expect(calls.map((c) => c.table).sort()).toEqual(
        ['fornecedores', 'orgaos', 'unidades'].sort(),
      );
    });

    function detectTable(sql: string): string {
      const entry = Object.entries(PARQUET_URL_BY_TABLE).find(([, url]) =>
        sql.includes(url),
      );
      return entry ? entry[0] : 'unknown';
    }
  });
});
