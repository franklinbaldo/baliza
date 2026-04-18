<script lang="ts">
  import { onMount } from 'svelte';
  import { getDuckDB } from '../lib/duckdb';
  import { FEATURED_QUERIES } from '../lib/homepage-content';
  import { getLatestParquetUrl } from '../lib/ia-manifest';
  import AlertBanner from './AlertBanner.svelte';

  let query = $state("SELECT * FROM contracts LIMIT 10");
  let results = $state<Record<string, unknown>[]>([]);
  let loading = $state(false);
  let error = $state<string | null>(null);
  let resolvedParquetUrl = $state<string | null>(null);

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

  onMount(async () => {
    resolvedParquetUrl = await getLatestParquetUrl();
  });

  $effect(() => {
    if (resolvedParquetUrl && query.includes("'IA_URL'")) {
      query = query.replaceAll(
        "'IA_URL'",
        `'${resolvedParquetUrl.replace(/'/g, "''")}'`,
      );
    }
  });

  async function runQuery() {
    loading = true;
    error = null;
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
</script>

<div class="explorer-card card">
  <div class="explorer-header">
    <div class="icon">🔍</div>
    <div class="text">
      <h3>SQL Explorer (Beta)</h3>
      <p>Consulte a base de contratos local usando SQL padrão.</p>
    </div>
  </div>

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

<style>
  .explorer-card { background: var(--color-base-100); border: 1px solid var(--color-base-300); padding: var(--space-md); border-radius: var(--radius-box); }
  .explorer-header { display: flex; gap: var(--space-md); margin-bottom: var(--space-md); }
  .explorer-header h3 { margin: 0; font-size: var(--font-size-lg); }
  .explorer-header p { margin: 0; font-size: var(--font-size-xs); color: var(--color-secondary); }

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
    border-radius: var(--radius-full);
    cursor: pointer;
    font-size: var(--font-size-xs);
    color: var(--color-secondary);
    font-family: var(--font-sans);
    transition: all var(--transition-base);
  }
  .fq-chip:hover { border-color: var(--color-primary); color: var(--color-primary); }

  textarea {
    width: 100%; height: 120px; background: var(--color-base-200); color: var(--font-mono);
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
