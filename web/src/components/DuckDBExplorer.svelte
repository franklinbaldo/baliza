<script lang="ts">
  import { onMount } from 'svelte';
  import { getDuckDB } from '../lib/duckdb';
  import { FEATURED_QUERIES } from '../lib/homepage-content';
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
    FEATURED_QUERIES.map((fq) => ({
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

<div class="explorer-card card">
  <div class="explorer-header">
    <div class="icon">🔍</div>
    <div class="text">
      <h3>SQL Explorer (Beta)</h3>
      <p>Consulte a base de contratos local usando SQL padrão.</p>
    </div>
    <button
      type="button"
      class="schema-toggle"
      onclick={() => (schemaOpen = !schemaOpen)}
      aria-expanded={schemaOpen}
      aria-controls="schema-sidebar"
    >
      {schemaOpen ? 'Ocultar esquema' : 'Mostrar esquema'}
    </button>
  </div>

  <div class="explorer-body" class:with-sidebar={schemaOpen}>
    {#if schemaOpen}
      <aside
        class="schema-sidebar"
        id="schema-sidebar"
        data-testid="schema-sidebar"
        aria-label="Esquema das tabelas disponíveis"
      >
        <h4>Tabelas disponíveis</h4>
        <ul class="schema-tables">
          {#each ARCHIVED_TABLES as table (table)}
            <li>
              <button
                type="button"
                class="schema-table-toggle"
                aria-expanded={!!expanded[table]}
                onclick={() => toggle(table)}
              >
                <span class="schema-table-name">{table}</span>
                <span class="schema-chevron" aria-hidden="true">{expanded[table] ? '▾' : '▸'}</span>
              </button>
              {#if expanded[table]}
                <ul class="schema-columns">
                  {#each SCHEMA_MAP[table] as col (col.name)}
                    <li>
                      <button
                        type="button"
                        class="schema-column"
                        onclick={() => insertFromColumn(table, col.name)}
                        title={`Inserir SELECT ${col.name}`}
                      >
                        <span class="schema-column-name">{col.name}</span>
                        <span class="schema-column-type">{col.type}</span>
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

    <div class="editor-column">
      <div class="featured-queries" role="group" aria-label="Consultas prontas">
        <span class="featured-label">Consultas prontas:</span>
        {#each featuredQueries as fq (fq.label)}
          <button class="fq-chip" onclick={() => (query = fq.sql)}>
            {fq.label}
          </button>
        {/each}
      </div>

      <div class="editor-zone">
        <textarea bind:value={query} spellcheck="false" aria-label="Consulta SQL"></textarea>
        {#if hasUnresolvedUrl}
          <div class="template-hint" role="note">
            ⚠️ Substitua <code>'IA_URL'</code> pela URL real do arquivo Parquet no Internet Archive antes de executar.
          </div>
        {/if}
        <button class="btn btn-primary" onclick={runQuery} disabled={loading || hasUnresolvedUrl}>
          {loading ? "Executando..." : "Explorar Dados"}
        </button>
      </div>

      {#if error}
        <div class="error-wrap">
          <AlertBanner title="Erro SQL" message={error} level="error" />
        </div>
      {/if}

      {#if results.length > 0}
        <div class="results-container">
          <div class="results-meta">
            <span>{results.length} linhas retornadas</span>
          </div>
          <div class="results-table-wrapper">
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

<style>
  .explorer-card { background: var(--color-base-100); border: 1px solid var(--color-base-300); padding: var(--space-md); border-radius: var(--radius-box); }
  .explorer-header {
    display: flex;
    gap: var(--space-md);
    align-items: flex-start;
    margin-bottom: var(--space-md);
  }
  .explorer-header h3 { margin: 0; font-size: var(--font-size-lg); }
  .explorer-header p { margin: 0; font-size: var(--font-size-xs); color: var(--color-secondary); }
  .explorer-header .text { flex: 1; }
  .schema-toggle {
    background: var(--color-base-200);
    border: 1px solid var(--color-base-300);
    border-radius: 0;
    padding: 4px 10px;
    font-size: var(--font-size-xs);
    cursor: pointer;
    color: var(--color-secondary);
    transition: all var(--transition-base);
  }
  .schema-toggle:hover, .schema-toggle:focus-visible { border-color: var(--color-primary); color: var(--color-primary); transform: translate(-1px, -1px); box-shadow: 2px 2px 0 var(--color-primary); }

  .explorer-body {
    display: grid;
    gap: var(--space-md);
    grid-template-columns: 1fr;
  }
  .explorer-body.with-sidebar {
    grid-template-columns: 240px 1fr;
  }
  @media (max-width: 720px) {
    .explorer-body.with-sidebar { grid-template-columns: 1fr; }
  }

  .schema-sidebar {
    background: var(--color-base-200);
    border: 1px solid var(--color-base-300);
    border-radius: var(--radius-sm);
    padding: var(--space-sm);
    max-height: 420px;
    overflow: auto;
    font-size: var(--font-size-xs);
  }
  .schema-sidebar h4 {
    margin: 0 0 var(--space-xs);
    font-size: var(--font-size-sm);
    color: var(--color-secondary);
    text-transform: uppercase;
    letter-spacing: 0.04em;
  }
  .schema-tables { list-style: none; padding: 0; margin: 0; display: grid; gap: 4px; }
  .schema-table-toggle {
    display: flex;
    justify-content: space-between;
    width: 100%;
    background: none;
    border: 1px solid var(--color-base-300);
    border-radius: var(--radius-sm);
    padding: 4px 8px;
    cursor: pointer;
    color: var(--color-base-content);
    font-family: var(--font-mono);
    font-size: var(--font-size-xs);
  }
  .schema-table-toggle:hover { border-color: var(--color-primary); }
  .schema-table-name { font-weight: 700; }
  .schema-columns {
    list-style: none;
    padding: 4px 0 4px var(--space-sm);
    margin: 0;
    display: grid;
    gap: 2px;
  }
  .schema-column {
    background: none;
    border: none;
    padding: 2px 4px;
    width: 100%;
    display: flex;
    justify-content: space-between;
    gap: var(--space-xs);
    cursor: pointer;
    font-family: var(--font-mono);
    font-size: var(--font-size-xs);
    color: var(--color-base-content);
    text-align: left;
  }
  .schema-column:hover { color: var(--color-primary); }
  .schema-column-type { color: var(--color-secondary); }

  .editor-column { min-width: 0; }

  .featured-queries {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: var(--space-xs);
    margin-bottom: var(--space-md);
    font-size: var(--font-size-xs);
    color: var(--color-secondary);
  }
  .featured-label { font-weight: 600; }
  .fq-chip {
    background: var(--color-base-200);
    border: 1px solid var(--color-base-300);
    padding: 3px 10px;
    border-radius: 0;
    cursor: pointer;
    font-size: var(--font-size-xs);
    color: var(--color-secondary);
    font-family: var(--font-sans);
    transition: all var(--transition-base);
  }
  .fq-chip:hover, .fq-chip:focus-visible { border-color: var(--color-primary); color: var(--color-primary); transform: translate(-1px, -1px); box-shadow: 2px 2px 0 var(--color-primary); }

  textarea {
    width: 100%; height: 120px; background: var(--color-base-200); color: var(--color-base-content);
    border: 1px solid var(--color-base-300); border-radius: var(--radius-sm); padding: var(--space-sm);
    font-family: var(--font-mono); font-size: var(--font-size-sm); resize: vertical; margin-bottom: var(--space-sm);
    outline: none;
  }
  textarea:focus { border-color: var(--color-primary); }

  .template-hint {
    font-size: var(--font-size-xs);
    color: var(--color-warning, #b45309);
    background: color-mix(in srgb, var(--color-warning, #b45309) 10%, transparent);
    border: 1px solid color-mix(in srgb, var(--color-warning, #b45309) 30%, transparent);
    border-radius: var(--radius-sm);
    padding: 6px 10px;
    margin-bottom: var(--space-sm);
  }
  .template-hint code { font-family: var(--font-mono); font-weight: 700; }

  .error-wrap { margin-top: var(--space-md); }

  .results-container { margin-top: var(--space-lg); border-top: 1px solid var(--color-base-300); padding-top: var(--space-md); }
  .results-meta { font-size: 0.7rem; color: var(--color-secondary); margin-bottom: 4px; font-weight: 700; text-transform: uppercase; }
  .results-table-wrapper { overflow-x: auto; border: 1px solid var(--color-base-300); border-radius: var(--radius-sm); }

  table { width: 100%; border-collapse: collapse; font-size: 0.75rem; }
  th { background: var(--color-base-200); text-align: left; padding: 8px; border-bottom: 1px solid var(--color-base-300); }
  td { padding: 8px; border-bottom: 1px solid var(--color-base-300); white-space: nowrap; }
  tr:last-child td { border-bottom: none; }
  tr:hover td { background: var(--color-base-200); }
</style>
