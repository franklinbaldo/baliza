<script lang="ts">
  import { onMount } from 'svelte';
  import { getDuckDB } from '../lib/duckdb';
  import { HOMEPAGE_SQL_EXAMPLES } from '../lib/homepage-content';
  import { getLatestParquetUrl } from '../lib/ia-manifest';
  import { SCHEMA_MAP } from '../lib/explorerSchema';
  import { ARCHIVED_TABLES, type ArchivedTable } from '../lib/archive/schema';
  import AlertBanner from './AlertBanner.svelte';

  function readInitialQuery(): string {
    if (typeof window === 'undefined') return 'SELECT * FROM contracts LIMIT 10';
    const fromUrl = new URLSearchParams(window.location.search).get('sql');
    return fromUrl && fromUrl.length > 0 ? fromUrl : 'SELECT * FROM contracts LIMIT 10';
  }

  let query = $state(readInitialQuery());
  let results = $state<Record<string, unknown>[]>([]);
  let loading = $state(false);
  let error = $state<string | null>(null);
  let resolvedParquetUrl = $state<string | null>(null);
  let resolvedUrls = $state<Partial<Record<ArchivedTable, string | null>>>({});
  let schemaOpen = $state(true);
  let expanded = $state<Record<string, boolean>>({ contratos: true });

  const featuredQueries = $derived(
    HOMEPAGE_SQL_EXAMPLES.map((fq) => ({
      ...fq,
      sql: resolvedParquetUrl
        ? fq.sql.replaceAll(
            "'IA_URL'",
            `'${resolvedParquetUrl.replace(/'/g, "''")}'`,
          )
        : fq.sql,
    })),
  );

  const hasUnresolvedUrl = $derived(query.includes("'IA_URL'"));

  // Embed mode: a third-party iframe passes ?sql=…&embed=true and expects the
  // query to auto-execute once the Parquet URL resolves. Read once on mount so
  // a user toggling query strings later does not re-trigger.
  const isEmbed = (() => {
    if (typeof window === 'undefined') return false;
    try {
      return new URLSearchParams(window.location.search).get('embed') === 'true';
    } catch {
      return false;
    }
  })();

  onMount(async () => {
    resolvedParquetUrl = await getLatestParquetUrl();
    // Resolve every archived table's latest Parquet URL once. Each
    // getLatestParquetUrl(t) call is memoized inside ia-manifest, so this is
    // effectively one shared manifest fetch plus 4 table lookups.
    const entries = await Promise.all(
      ARCHIVED_TABLES.map(async (t) => [t, await getLatestParquetUrl(t)] as const),
    );
    resolvedUrls = Object.fromEntries(entries) as Partial<Record<ArchivedTable, string | null>>;

    if (isEmbed && !query.includes("'IA_URL'")) {
      void runQuery();
    }
  });

  $effect(() => {
    if (resolvedParquetUrl && query.includes("'IA_URL'")) {
      query = query.replaceAll(
        "'IA_URL'",
        `'${resolvedParquetUrl.replace(/'/g, "''")}'`,
      );
    }
  });

  function pushSqlToUrl(sql: string) {
    if (typeof window === 'undefined') return;
    // eslint-disable-next-line svelte/prefer-svelte-reactivity
    const params = new URLSearchParams(window.location.search);
    params.set('sql', sql);
    window.history.replaceState({}, '', `${window.location.pathname}?${params.toString()}`);
  }

  async function runQuery() {
    loading = true;
    error = null;
    pushSqlToUrl(query);
    try {
      const { conn } = await getDuckDB();
      const res = await conn.query(query);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      results = res.toArray().map((r: any) => r.toJSON());
    } catch (err: unknown) {
      console.error(err);
      error = err instanceof Error ? err.message : "Erro ao executar consulta SQL.";
    } finally {
      loading = false;
    }
  }

  function toggle(table: string) {
    expanded = { ...expanded, [table]: !expanded[table] };
  }

  function insertFromColumn(table: ArchivedTable, column: string) {
    // Use the URL for the table the column came from, not the default
    // contratos URL. Orgaos/unidades/fornecedores columns don't exist in the
    // contratos parquet, so pointing there would make the generated query
    // error instead of showing the column.
    //
    // Early-return if the per-table URL hasn't resolved yet. We intentionally
    // do NOT fall back to 'IA_URL' here: the reactive $effect would then
    // rewrite it to resolvedParquetUrl (always contratos), silently producing
    // wrong-table SQL for orgaos/unidades/fornecedores columns.
    const url = resolvedUrls[table];
    if (!url) return;
    const escaped = url.replace(/'/g, "''");
    query = `SELECT ${column} FROM read_parquet('${escaped}') LIMIT 10`;
    // Hook for component assertions that need to wait for the column click to
    // resolve into textarea state.
    if (typeof window !== 'undefined') {
      window.setTimeout(() => {
        const ta = document.querySelector('.editor-zone textarea') as HTMLTextAreaElement | null;
        if (ta) ta.focus();
      }, 0);
    }
  }
</script>

<div>
  <div>
    <div>🔍</div>
    <div>
      <h3>SQL Explorer (Beta)</h3>
      <p>Consulte a base de contratos local usando SQL padrão.</p>
    </div>
    <button
      type="button"
      onclick={() => (schemaOpen = !schemaOpen)}
      aria-expanded={schemaOpen}
      aria-controls="schema-sidebar"
    >
      {schemaOpen ? 'Ocultar esquema' : 'Mostrar esquema'}
    </button>
  </div>

  <div>
    {#if schemaOpen}
      <aside
        id="schema-sidebar"
        data-testid="schema-sidebar"
        aria-label="Esquema das tabelas disponíveis"
      >
        <h4>Tabelas disponíveis</h4>
        <ul>
          {#each ARCHIVED_TABLES as table (table)}
            <li>
              <button
                type="button"
                aria-expanded={!!expanded[table]}
                onclick={() => toggle(table)}
              >
                <span>{table}</span>
                <span aria-hidden="true">{expanded[table] ? '▾' : '▸'}</span>
              </button>
              {#if expanded[table]}
                <ul>
                  {#each SCHEMA_MAP[table] as col (col.name)}
                    <li>
                      <button
                        type="button"
                        onclick={() => insertFromColumn(table, col.name)}
                        title={`Inserir SELECT ${col.name}`}
                      >
                        <span>{col.name}</span>
                        <span>{col.type}</span>
                      </button>
                    </li>
                  {/each}
                </ul>
              {/if}
            </li>
          {/each}
        </ul>
      </aside>
    {/if}

    <div>
      <div role="group" aria-label="Consultas prontas">
        <span>Consultas prontas:</span>
        {#each featuredQueries as fq (fq.label)}
          <button onclick={() => (query = fq.sql)}>
            {fq.label}
          </button>
        {/each}
      </div>

      <div>
        <textarea bind:value={query} spellcheck="false" aria-label="Consulta SQL"></textarea>
        {#if hasUnresolvedUrl}
          <div role="note">
            ⚠️ Substitua <code>'IA_URL'</code> pela URL real do arquivo Parquet no Internet Archive antes de executar.
          </div>
        {/if}
        <button onclick={runQuery} disabled={loading || hasUnresolvedUrl}>
          {loading ? "Executando..." : "Explorar Dados"}
        </button>
      </div>

      {#if error}
        <div>
          <AlertBanner title="Erro SQL" message={error} level="error" />
        </div>
      {/if}

      {#if results.length > 0}
        <div>
          <div>
            <span>{results.length} linhas retornadas</span>
          </div>
          <div>
            <table>
              <thead>
                <tr>
                  {#each Object.keys(results[0]) as col (col)}
                    <th>{col}</th>
                  {/each}
                </tr>
              </thead>
              <tbody>
                {#each results as row, i (i)}
                  <tr>
                    {#each Object.values(row) as val, j (`${i}-${j}`)}
                      <td>{val}</td>
                    {/each}
                  </tr>
                {/each}
              </tbody>
            </table>
          </div>
        </div>
      {/if}
    </div>
  </div>
</div>

