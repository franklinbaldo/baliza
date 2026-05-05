<script lang="ts">
  import { onMount } from 'svelte';
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { getQueryClient } from '../lib/queryClient';
  import { fetchPublicacaoPagesForObjeto, fetchPublicacaoList } from '../lib/pncpPublicacao';
  import { archivedContratoToInternalContract, type PNCPContract } from '../lib/pncp';
  import { queryParquetFallback } from '../lib/parquetFallback';
  import type { ArchivedContrato } from '../lib/archive/schema';
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
  import { readFilters, toApiParams } from '../lib/searchFilters';

  setQueryClientContext(getQueryClient());

  const { id }: { id: string } = $props();

  // Snapshot the previous-visit cutoff on mount and *do not* stamp the
  // new timestamp yet — markVisited fires only after the fetch succeeds
  // so a transient PNCP failure cannot silently advance the cutoff and
  // hide contracts published between the last good visit and a failed
  // attempt.
  let previousVisited = $state<string | undefined>(undefined);
  let mounted = $state(false);
  let visitStamped = false;
  // Snapshot of query.dataUpdatedAt at mount. TanStack Query reports
  // isSuccess=true synchronously when cached data is available, so a
  // naive isSuccess gate would stamp the cutoff before the refetch
  // completes (and could advance it even if that refetch then fails).
  // We compare against the mount-time timestamp so the stamp only fires
  // once a fetch that started during this view's lifetime has resolved.
  let initialDataUpdatedAt = $state(0);
  onMount(() => {
    hydrateWatches();
    const e = watchState.entries.find(w => w.id === id);
    previousVisited = e?.lastVisited;
    initialDataUpdatedAt = query.dataUpdatedAt ?? 0;
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
      // PNCP swagger uses bare `cnpj` (NOT `cnpjOrgao`) for the org filter
      // on /contratacoes/publicacao — see PublicacaoFilters in pncpPublicacao.ts.
      if (e.type === 'agency') return fetchPublicacaoList({ cnpj: e.filter });
      if (e.type === 'supplier') {
        // PNCP has no supplier-by-CNPJ filter on /publicacao, so we mirror
        // SupplierDetailView and read the parquet snapshot, mapping rows
        // back into PNCPContract for a uniform render path.
        const archived = await queryParquetFallback<ArchivedContrato>(
          'ni_fornecedor',
          e.filter,
          50,
          'data_publicacao_pncp',
        );
        if (archived.ok) return archived.rows.map((row) => archivedContratoToInternalContract(row));
        return [];
      }
      // 'query' watches store a canonical URLSearchParams string with `q`
      // plus any advanced filters that were active when the watch was
      // created (cnpj, ibge, uf, …). Replay both so the re-fetch matches
      // the user's saved definition rather than a broader free-text search.
      const params = new URLSearchParams(e.filter);
      // Filter-only watches (created from advanced filters with no
      // free-text) have no `q` in the saved key, and their label may be
      // the serialized filter string itself — falling back to the label
      // would feed `cnpj=…` into the object-text matcher and return zero
      // rows. Pass empty term so fetchPublicacaoPagesForObjeto runs the
      // filter-only path.
      const term = params.get('q') ?? '';
      const filters = readFilters(params);
      return fetchPublicacaoPagesForObjeto(term, { filters: toApiParams(filters) });
    },
  }));

  // Stamp the new lastVisited only after a fresh fetch resolves.
  // Conditions:
  //   • dataUpdatedAt advanced past the mount-time value → the success
  //     belongs to a fetch that started in this session, not a cache hit.
  //   • !isFetching → the resolved data is the most recent in flight,
  //     so a follow-up failure can still preserve the prior cutoff.
  $effect(() => {
    if (
      !visitStamped &&
      mounted &&
      entry &&
      query.isSuccess &&
      !query.isFetching &&
      (query.dataUpdatedAt ?? 0) > initialDataUpdatedAt
    ) {
      visitStamped = true;
      markVisited(id);
    }
  });

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
