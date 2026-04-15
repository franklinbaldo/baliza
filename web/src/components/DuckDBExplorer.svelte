<script lang="ts">
  import * as duckdb from '@duckdb/duckdb-wasm';
  import { fade } from 'svelte/transition';
  import { getDuckDB } from '../lib/duckdb';

  let query = $state("SELECT * FROM contracts LIMIT 10");
  let results = $state<Record<string, unknown>[]>([]);
  let loading = $state(false);
  let error = $state<string | null>(null);

  async function runQuery() {
    loading = true;
    error = null;
    try {
      const { conn } = await getDuckDB();
      const res = await conn.query(query);
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

  <div class="editor-zone">
    <textarea bind:value={query} spellcheck="false"></textarea>
    <button class="btn btn-primary" onclick={runQuery} disabled={loading}>
      {loading ? "Executando..." : "Explorar Dados"}
    </button>
  </div>

  {#if error}
    <div class="error-msg" in:fade>
      <strong>Erro SQL:</strong> {error}
    </div>
  {/if}

  {#if results.length > 0}
    <div class="results-container" in:fade>
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
  
  textarea {
    width: 100%; height: 120px; background: var(--color-base-200); color: var(--font-mono);
    border: 1px solid var(--color-base-300); border-radius: var(--radius-sm); padding: var(--space-sm);
    font-family: var(--font-mono); font-size: var(--font-size-sm); resize: vertical; margin-bottom: var(--space-sm);
    outline: none;
  }
  textarea:focus { border-color: var(--color-primary); }

  .results-container { margin-top: var(--space-lg); border-top: 1px solid var(--color-base-300); padding-top: var(--space-md); }
  .results-meta { font-size: 0.7rem; color: var(--color-secondary); margin-bottom: 4px; font-weight: 700; text-transform: uppercase; }
  .results-table-wrapper { overflow-x: auto; border: 1px solid var(--color-base-300); border-radius: var(--radius-sm); }
  
  table { width: 100%; border-collapse: collapse; font-size: 0.75rem; }
  th { background: var(--color-base-200); text-align: left; padding: 8px; border-bottom: 1px solid var(--color-base-300); }
  td { padding: 8px; border-bottom: 1px solid var(--color-base-300); white-space: nowrap; }
  tr:last-child td { border-bottom: none; }
  tr:hover td { background: var(--color-base-200); }

  .error-msg { margin-top: var(--space-md); padding: var(--space-sm); background: #fee2e2; color: #991b1b; border-radius: var(--radius-sm); font-size: var(--font-size-sm); }
</style>
