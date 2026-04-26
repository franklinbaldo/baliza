<script lang="ts" generics="T">
  import type { Snippet } from 'svelte';

  const { 
    items, 
    pageSize = 10,
    resetTrigger,
    children
  }: { 
    items: T[], 
    pageSize?: number,
    resetTrigger?: unknown,
    children: Snippet<[T[]]>
  } = $props();

  let currentPage = $state(1);

  // Allow forcing a reset from the outside (e.g. when filters change)
  $effect(() => {
    resetTrigger;
    currentPage = 1;
  });

  const totalPages = $derived(Math.max(1, Math.ceil(items.length / pageSize)));
  const paginatedItems = $derived(
    items.slice((currentPage - 1) * pageSize, currentPage * pageSize)
  );

  function prev() {
    if (currentPage > 1) {
      currentPage -= 1;
      window.scrollTo({ top: 0, behavior: 'smooth' });
    }
  }

  function next() {
    if (currentPage < totalPages) {
      currentPage += 1;
      window.scrollTo({ top: 0, behavior: 'smooth' });
    }
  }
</script>

{@render children(paginatedItems)}

{#if totalPages > 1}
  <div class="pagination-controls">
    <button class="btn btn-outline" disabled={currentPage === 1} onclick={prev}>Anterior</button>
    <span class="page-info">Página {currentPage} de {totalPages}</span>
    <button class="btn btn-outline" disabled={currentPage === totalPages} onclick={next}>Próxima</button>
  </div>
{/if}

<style>
  .pagination-controls {
    display: flex;
    justify-content: center;
    align-items: center;
    gap: var(--space-md);
    margin-top: var(--space-xl);
  }
  .page-info {
    font-size: var(--font-size-sm);
    color: var(--color-secondary);
  }
</style>
