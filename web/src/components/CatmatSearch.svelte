<script lang="ts">
  import { searchCatmat, type CatmatEntry } from '../lib/catmat';
  import { resolve } from '../lib/baseUrl';

  let searchInput = $state('');
  let results: CatmatEntry[] = $state([]);
  let error: string | null = $state(null);

  // Track the latest query so a slow earlier fetch can't overwrite a later one.
  let activeQueryId = 0;

  $effect(() => {
    const trimmed = searchInput.trim();
    if (trimmed.length < 2) {
      ++activeQueryId; // cancel any in-flight search so it can't repopulate stale results
      results = [];
      error = null;
      return;
    }
    const id = ++activeQueryId;
    void searchCatmat(trimmed).then(
      (matches) => {
        if (id === activeQueryId) {
          results = matches;
          error = null;
        }
      },
      (err: unknown) => {
        if (id === activeQueryId) {
          results = [];
          error = err instanceof Error ? err.message : String(err);
        }
      },
    );
  });
</script>

<search>
  <label for="catmat-input">
    Descrição do item para busca CATMAT
    <input
      id="catmat-input"
      type="search"
      bind:value={searchInput}
      placeholder="Ex.: papel sulfite A4 75g, caneta esferográfica..."
    />
  </label>
  {#if error}
    <small role="alert">Não foi possível carregar o catálogo: {error}</small>
  {:else if results.length > 0}
    <ul data-testid="catmat-results" role="listbox">
      {#each results as entry (entry.type + ':' + entry.code)}
        <li data-testid="catmat-result-item" role="option" aria-selected="false">
          <code>{entry.code}</code>
          <small data-badge>{entry.type}</small>
          <p>{entry.description}</p>
          <a
            data-testid="pca-demand-link"
            href={resolve(`pca?codigo=${encodeURIComponent(entry.code)}`)}
          >Ver demanda planejada (PCA) →</a>
        </li>
      {/each}
    </ul>
  {/if}
</search>

