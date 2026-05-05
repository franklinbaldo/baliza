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

  // Allow forcing a reset from the outside (e.g. when filters change).
  // Void-cast reads resetTrigger to register the dependency without
  // triggering the no-unused-expressions lint rule.
  $effect(() => {
    void resetTrigger;
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
  <div>
    <button disabled={currentPage === 1} onclick={prev}>Anterior</button>
    <span>Página {currentPage} de {totalPages}</span>
    <button disabled={currentPage === totalPages} onclick={next}>Próxima</button>
  </div>
{/if}

