<script lang="ts">
  import { onMount } from 'svelte';
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { getQueryClient } from '../lib/queryClient';
  import { fetchPublicacaoPagesForObjeto, fetchPublicacaoList } from '../lib/pncpPublicacao';
  import type { PNCPContract } from '../lib/pncp';
  import { resolve } from '../lib/baseUrl';
  import { formatBRL, formatDate } from '../lib/format';
  import EmptyState from './EmptyState.svelte';
  import AlertBanner from './AlertBanner.svelte';
  import {
    hydrateWatches,
    markVisited,
    watchState,
    type WatchEntry,
  } from '../lib/watchStore.svelte';

  setQueryClientContext(getQueryClient());

  const { id }: { id: string } = $props();

  // Capture the previous-visit cutoff exactly once on mount, BEFORE
  // markVisited writes the new timestamp — otherwise the cutoff and the
  // freshly stamped value would coincide and the diff section would
  // always render zero matches.
  let previousVisited = $state<string | undefined>(undefined);
  let mounted = $state(false);
  onMount(() => {
    hydrateWatches();
    previousVisited = markVisited(id);
    mounted = true;
  });

  const entry = $derived<WatchEntry | undefined>(
    mounted ? watchState.entries.find(w => w.id === id) : undefined,
  );

  const query = createQuery(() => ({
    queryKey: ['watch-detail', id],
    enabled: mounted && !!entry,
    queryFn: async (): Promise<PNCPContract[]> => {
      const e = entry!;
      if (e.type === 'agency') return fetchPublicacaoList({ orgaoCnpj: e.filter });
      if (e.type === 'supplier') return fetchPublicacaoList({ niFornecedor: e.filter });
      // 'query' watches store a URLSearchParams string with at least `q=…`;
      // the label captures the human-readable term used to create them, so
      // it's the most faithful free-text input to re-run.
      return fetchPublicacaoPagesForObjeto(e.label || e.filter);
    },
  }));

  const results = $derived(query.data ?? []);
  const loading = $derived(query.isFetching);
  const error = $derived(query.error as Error | null);

  // Items strictly newer than the previous visit cutoff. Empty when this
  // is the first visit, when nothing has been published since, or while
  // the fetch is still pending.
  const newSinceVisit = $derived(
    !previousVisited
      ? []
      : results.filter(c => (c.dataPublicacaoPncp ?? '') > previousVisited!),
  );

  function linkFor(c: PNCPContract): string {
    return resolve(`contratacao?id=${c.numeroControlePNCP ?? ''}`);
  }
</script>

<section class="watch-detail container">
  {#if mounted && !entry}
    <EmptyState
      title="Vigilância não encontrada"
      message="Esta vigilância não está salva neste navegador."
    />
  {:else if entry}
    <header class="head">
      <span class="kicker">Vigilância</span>
      <h1>{entry.label}</h1>
      <p class="meta">{entry.filter}</p>
    </header>

    {#if previousVisited}
      <section
        class="diff"
        data-testid="watch-diff-section"
        aria-labelledby="watch-diff-title"
      >
        <h2 id="watch-diff-title">Novidades desde sua última visita</h2>
        {#if newSinceVisit.length === 0}
          <p class="diff-empty">
            Nenhuma novidade desde {formatDate(previousVisited)}.
          </p>
        {:else}
          <ul class="diff-list">
            {#each newSinceVisit as c (c.numeroControlePNCP)}
              <li data-testid="watch-diff-item">
                <a href={linkFor(c)}>
                  <span class="diff-title">{c.objetoContratacao}</span>
                  <span class="diff-meta">
                    <span>{formatDate(c.dataPublicacaoPncp ?? '')}</span>
                    <span>{formatBRL(c.valorTotalEstimado ?? null)}</span>
                  </span>
                </a>
              </li>
            {/each}
          </ul>
        {/if}
      </section>
    {/if}

    <section class="all" aria-labelledby="watch-all-title">
      <h2 id="watch-all-title">Todos os resultados</h2>
      {#if loading}
        <p class="muted">Buscando…</p>
      {:else if error}
        <AlertBanner title="Não foi possível buscar" message={error.message} level="error" />
      {:else if results.length === 0}
        <EmptyState title="Sem resultados" message="A vigilância ainda não tem correspondências." />
      {:else}
        <ul class="all-list" data-testid="watch-all-results">
          {#each results as c (c.numeroControlePNCP)}
            <li>
              <a href={linkFor(c)}>
                <span>{c.objetoContratacao}</span>
                <span class="muted">{formatDate(c.dataPublicacaoPncp ?? '')}</span>
              </a>
            </li>
          {/each}
        </ul>
      {/if}
    </section>
  {/if}
</section>

<style>
  .watch-detail { padding: var(--space-2xl) 0; display: grid; gap: var(--space-lg); }
  .head { border-bottom: 2px solid var(--color-base-300); padding-bottom: var(--space-md); }
  .kicker {
    font-family: var(--font-mono);
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--color-secondary);
  }
  h1 { font-size: var(--font-size-2xl); margin: 4px 0 0; }
  .meta { font-family: var(--font-mono); font-size: var(--font-size-sm); color: var(--color-secondary); margin: 4px 0 0; }
  .diff { padding: var(--space-md); border: 1px solid var(--color-primary); border-radius: var(--radius-sm); background: var(--color-base-100); }
  .diff h2 { font-size: var(--font-size-lg); margin: 0 0 var(--space-sm); }
  .diff-empty { color: var(--color-secondary); margin: 0; }
  .diff-list, .all-list { list-style: none; margin: 0; padding: 0; display: grid; gap: var(--space-sm); }
  .diff-list a, .all-list a {
    display: grid;
    gap: 4px;
    padding: var(--space-sm);
    border: 1px solid var(--color-base-300);
    text-decoration: none;
    color: inherit;
  }
  .diff-meta { display: flex; justify-content: space-between; font-size: var(--font-size-sm); color: var(--color-secondary); font-family: var(--font-mono); }
  .diff-title { font-weight: 600; }
  .all h2 { font-size: var(--font-size-lg); margin: 0 0 var(--space-sm); }
  .muted { color: var(--color-secondary); }
</style>
