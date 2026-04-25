<script lang="ts">
  import { searchCatmat, type CatmatEntry } from '../lib/catmat';

  let searchInput = $state('');
  let results: CatmatEntry[] = $state([]);
  let error: string | null = $state(null);

  // Track the latest query so a slow earlier fetch can't overwrite a later one.
  let activeQueryId = 0;

  $effect(() => {
    const trimmed = searchInput.trim();
    if (trimmed.length < 3) {
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

<div class="catmat-search">
  <label for="catmat-input">Descrição do item para busca CATMAT</label>
  <input
    id="catmat-input"
    type="search"
    bind:value={searchInput}
    placeholder="Ex.: papel sulfite A4 75g, caneta esferográfica..."
  />
  {#if error}
    <p class="catmat-error" role="alert">Não foi possível carregar o catálogo: {error}</p>
  {:else if results.length > 0}
    <ul data-testid="catmat-results">
      {#each results as entry (entry.type + ':' + entry.code)}
        <li data-testid="catmat-result-item">
          <span class="catmat-code">{entry.code}</span>
          <span class="catmat-type">{entry.type}</span>
          <span class="catmat-desc">{entry.description}</span>
        </li>
      {/each}
    </ul>
  {/if}
</div>

<style>
  .catmat-search {
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
    max-width: 48rem;
  }

  label {
    font-weight: 600;
    font-size: 0.875rem;
  }

  input {
    padding: 0.5rem 0.75rem;
    border: 1px solid #d1d5db;
    border-radius: 0.375rem;
    font-size: 1rem;
    width: 100%;
  }

  ul {
    list-style: none;
    margin: 0;
    padding: 0;
    border: 1px solid #e5e7eb;
    border-radius: 0.375rem;
    overflow: hidden;
  }

  li {
    display: grid;
    grid-template-columns: 8rem 5rem 1fr;
    gap: 0.5rem;
    padding: 0.625rem 0.75rem;
    font-size: 0.875rem;
    border-bottom: 1px solid #f3f4f6;
  }

  li:last-child {
    border-bottom: none;
  }

  .catmat-code {
    font-family: monospace;
    font-weight: 600;
    color: #374151;
  }

  .catmat-type {
    font-size: 0.75rem;
    font-weight: 500;
    color: #6b7280;
    text-transform: uppercase;
    align-self: center;
  }

  .catmat-desc {
    color: #111827;
  }

  .catmat-error {
    color: #b91c1c;
    font-size: 0.875rem;
  }
</style>
