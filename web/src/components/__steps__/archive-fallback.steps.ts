import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { vi, expect } from 'vitest';

// IMPORTANT: this file intentionally does NOT import ./shared. shared.ts
// globally mocks queryParquetFallback; here we exercise the REAL
// queryArchivedTable implementation and control duckdb + fetch directly.

const feature = await loadFeature('features/archive-fallback.feature');

interface DuckDBHandle {
  db: null;
  conn: {
    query: ReturnType<typeof vi.fn>;
    cancelSent: ReturnType<typeof vi.fn>;
  };
}

let queryMock: ReturnType<typeof vi.fn>;
let cancelSentMock: ReturnType<typeof vi.fn>;
let duckdbInitCount = 0;
let duckdbInstance: DuckDBHandle | null = null;
let duckdbShouldThrow = false;

async function fakeGetDuckDB(): Promise<DuckDBHandle> {
  if (duckdbShouldThrow) throw new Error('duckdb boom');
  if (!duckdbInstance) {
    duckdbInitCount += 1;
    duckdbInstance = {
      db: null,
      conn: { query: queryMock, cancelSent: cancelSentMock },
    };
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
  cancelSentMock = vi.fn().mockResolvedValue(true);
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
    opts: { limit?: number; orderByColumn?: string; timeoutMs?: number } = {},
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

  Scenario('Served queries log "[archive] fallback served" with rowCount', ({ Given, And, When, Then }) => {
    let infoSpy: ReturnType<typeof vi.spyOn>;
    Given('the IA manifest resolves to a parquet url', () => {
      installManifestFetch(200, manifestCsvAllTables());
      infoSpy = vi.spyOn(console, 'info').mockImplementation(() => {});
    });
    And('DuckDB returns two rows', () => {
      queryMock.mockResolvedValue({
        toArray: () => [
          { toJSON: () => ({ numero_controle_pncp: 'a' }) },
          { toJSON: () => ({ numero_controle_pncp: 'b' }) },
        ],
      });
    });
    When('queryArchivedTable is called for "contratos" filtered by "cnpj_orgao"', async () => {
      result = await callArchive('contratos', 'cnpj_orgao');
    });
    Then('console.info should log "[archive] fallback served" with rowCount 2', () => {
      const served = (infoSpy.mock.calls as unknown[][]).find((c) => c[0] === '[archive] fallback served');
      expect(served).toBeDefined();
      expect(served?.[1]).toEqual({
        table: 'contratos',
        column: 'cnpj_orgao',
        rowCount: 2,
      });
    });
    And('console.info should not log "[archive] fallback failed"', () => {
      const failed = (infoSpy.mock.calls as unknown[][]).find((c) => c[0] === '[archive] fallback failed');
      expect(failed).toBeUndefined();
    });
  });

  Scenario('Failed queries log "[archive] fallback failed" with reason', ({ Given, And, When, Then }) => {
    let infoSpy: ReturnType<typeof vi.spyOn>;
    Given('the IA manifest resolves to a parquet url', () => {
      installManifestFetch(200, manifestCsvAllTables());
      infoSpy = vi.spyOn(console, 'info').mockImplementation(() => {});
    });
    And('DuckDB returns no rows', () => {
      queryMock.mockResolvedValue({ toArray: () => [] });
    });
    When('queryArchivedTable is called for "contratos" filtered by "cnpj_orgao"', async () => {
      result = await callArchive('contratos', 'cnpj_orgao');
    });
    Then('console.info should log "[archive] fallback failed" with reason "empty"', () => {
      const failed = (infoSpy.mock.calls as unknown[][]).find((c) => c[0] === '[archive] fallback failed');
      expect(failed).toBeDefined();
      expect(failed?.[1]).toEqual({
        table: 'contratos',
        column: 'cnpj_orgao',
        reason: 'empty',
      });
    });
  });

  Scenario('Manifest 503 then 200 recovers via one retry', ({ Given, When, Then, And }) => {
    Given(
      'the IA manifest endpoint returns 503 on the first call and 200 on the second',
      () => {
        let call = 0;
        queryMock.mockResolvedValue({
          toArray: () => [{ toJSON: () => ({ numero_controle_pncp: 'x' }) }],
        });
        const fn = vi.fn().mockImplementation(() => {
          call += 1;
          if (call === 1) {
            return Promise.resolve(new Response('', { status: 503 }));
          }
          return Promise.resolve(
            new Response(manifestCsvAllTables(), {
              status: 200,
              headers: { 'Content-Type': 'text/csv' },
            }),
          );
        });
        fetchSpy = fn;
        global.fetch = fn as unknown as typeof fetch;
      },
    );
    When('queryArchivedTable is called for "contratos" filtered by "cnpj_orgao"', async () => {
      result = await callArchive('contratos', 'cnpj_orgao');
    });
    Then('the manifest endpoint should have been fetched twice', () => {
      expect(fetchSpy).toHaveBeenCalledTimes(2);
    });
    And('the result should succeed', () => {
      expect(result?.ok).toBe(true);
    });
  });

  Scenario('Manifest 404 is terminal and does not retry', ({ Given, When, Then }) => {
    Given('the IA manifest endpoint returns 404', () => {
      fetchSpy = installManifestFetch(404, '');
    });
    When('queryArchivedTable is called for "contratos" filtered by "cnpj_orgao"', async () => {
      result = await callArchive('contratos', 'cnpj_orgao');
    });
    Then('the manifest endpoint should have been fetched once', () => {
      expect(fetchSpy).toHaveBeenCalledTimes(1);
      expect(result).toEqual({ ok: false, reason: 'no_manifest' });
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
    And('DuckDB cancelSent should have been called to cancel the stuck query', () => {
      expect(cancelSentMock).toHaveBeenCalledTimes(1);
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

  Scenario('prefetchArchive warms the manifest without booting DuckDB', ({ Given, When, Then }) => {
    Given('the IA manifest resolves to a parquet url', () => {
      fetchSpy = installManifestFetch(200, manifestCsvAllTables());
      queryMock.mockResolvedValue({
        toArray: () => [{ toJSON: () => ({ numero_controle_pncp: 'x' }) }],
      });
    });
    When('prefetchArchive is called for "contratos"', async () => {
      const { prefetchArchive } = await import('../../lib/parquetFallback');
      prefetchArchive('contratos');
      // Let the fire-and-forget manifest fetch settle before the assertion.
      await new Promise((r) => setTimeout(r, 0));
      await new Promise((r) => setTimeout(r, 0));
    });
    Then('DuckDB should not have been initialized', () => {
      expect(duckdbInitCount).toBe(0);
      expect(getDuckDBSpy).not.toHaveBeenCalled();
    });
  });

  Scenario('A later queryArchivedTable after prefetchArchive reuses the warmed manifest', ({ Given, When, Then, And }) => {
    Given('prefetchArchive has warmed the manifest for "contratos"', async () => {
      fetchSpy = installManifestFetch(200, manifestCsvAllTables());
      queryMock.mockResolvedValue({
        toArray: () => [{ toJSON: () => ({ numero_controle_pncp: 'x' }) }],
      });
      const { prefetchArchive } = await import('../../lib/parquetFallback');
      prefetchArchive('contratos');
      await new Promise((r) => setTimeout(r, 0));
      await new Promise((r) => setTimeout(r, 0));
    });
    When('queryArchivedTable is called for "contratos" filtered by "cnpj_orgao"', async () => {
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

  Scenario('Malformed manifest rows are skipped with a warning', ({ Given, When, Then, And }) => {
    let warnSpy: ReturnType<typeof vi.spyOn>;
    let manifestResult: Awaited<
      ReturnType<typeof import('../../lib/ia-manifest').getLatestParquetInfo>
    > = null;

    Given(
      'the IA manifest endpoint returns a CSV with a valid contratos row and a malformed row missing parquet_url',
      () => {
        warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
        installManifestFetch(
          200,
          [
            'data_particao,table_name,parquet_url',
            '2024-12-01,contratos,',
            `2024-12-02,contratos,${PARQUET_URL_BY_TABLE.contratos}`,
          ].join('\n'),
        );
      },
    );
    When('getLatestParquetInfo is called for "contratos"', async () => {
      const { getLatestParquetInfo } = await import('../../lib/ia-manifest');
      manifestResult = await getLatestParquetInfo('contratos');
    });
    Then("the valid row's parquet url should be returned", () => {
      expect(manifestResult).toEqual({
        url: PARQUET_URL_BY_TABLE.contratos,
        dataParticao: '2024-12-02',
      });
    });
    And(
      'console.warn should log "[ia-manifest] skipping malformed row" once',
      () => {
        const malformed = (warnSpy.mock.calls as unknown[][]).filter(
          (c) => c[0] === '[ia-manifest] skipping malformed row',
        );
        expect(malformed).toHaveLength(1);
      },
    );
  });

  function detectTable(sql: string): string {
    const entry = Object.entries(PARQUET_URL_BY_TABLE).find(([, url]) =>
      sql.includes(url),
    );
    return entry ? entry[0] : 'unknown';
  }

  function installManifestAndCapture(): Array<{ table: string; sql: string }> {
    const calls: Array<{ table: string; sql: string }> = [];
    fetchSpy = installManifestFetch(200, manifestCsvAllTables());
    queryMock.mockImplementation(async (sql: string) => {
      calls.push({ table: detectTable(sql), sql });
      return { toArray: () => [{ toJSON: () => ({ cnpj: '00' }) }] };
    });
    return calls;
  }

  Scenario('queryArchivedOrgaos requests the orgaos parquet snapshot', ({ Given, When, Then }) => {
    let calls: Array<{ table: string; sql: string }>;
    Given('the IA manifest resolves to a parquet url for every table', () => {
      calls = installManifestAndCapture();
    });
    When('queryArchivedOrgaos is called', async () => {
      const { queryArchivedOrgaos } = await import('../../lib/parquetFallback');
      await queryArchivedOrgaos('cnpj', '00000000000191');
    });
    Then('the query should be routed to the "orgaos" parquet snapshot', () => {
      expect(calls).toHaveLength(1);
      expect(calls[0].table).toBe('orgaos');
    });
  });

  Scenario('queryArchivedUnidades requests the unidades parquet snapshot', ({ Given, When, Then }) => {
    let calls: Array<{ table: string; sql: string }>;
    Given('the IA manifest resolves to a parquet url for every table', () => {
      calls = installManifestAndCapture();
    });
    When('queryArchivedUnidades is called', async () => {
      const { queryArchivedUnidades } = await import('../../lib/parquetFallback');
      await queryArchivedUnidades('codigo_unidade', '1');
    });
    Then('the query should be routed to the "unidades" parquet snapshot', () => {
      expect(calls).toHaveLength(1);
      expect(calls[0].table).toBe('unidades');
    });
  });

  Scenario('queryArchivedFornecedores requests the fornecedores parquet snapshot', ({ Given, When, Then }) => {
    let calls: Array<{ table: string; sql: string }>;
    Given('the IA manifest resolves to a parquet url for every table', () => {
      calls = installManifestAndCapture();
    });
    When('queryArchivedFornecedores is called', async () => {
      const { queryArchivedFornecedores } = await import('../../lib/parquetFallback');
      await queryArchivedFornecedores('ni_fornecedor', '00000000000191');
    });
    Then('the query should be routed to the "fornecedores" parquet snapshot', () => {
      expect(calls).toHaveLength(1);
      expect(calls[0].table).toBe('fornecedores');
    });
  });
});
