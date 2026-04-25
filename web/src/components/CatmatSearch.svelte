<script lang="ts">
  import { searchCatmat } from '../lib/catmat';

  let searchInput = $state('');
  const results = $derived(
    searchInput.trim().length >= 2 ? searchCatmat(searchInput.trim()) : [],
  );
</script>

<div class="catmat-search">
  <label for="catmat-input">Descrição do item</label>
  <input
    id="catmat-input"
    type="search"
    bind:value={searchInput}
    aria-label="Descrição do item para busca CATMAT"
    placeholder="Ex.: papel sulfite A4 75g, caneta esferográfica..."
  />
  {#if results.length > 0}
    <ul data-testid="catmat-results">
      {#each results as entry (entry.code)}
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
</style>
