<script lang="ts">
  import { onMount } from 'svelte';
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { getQueryClient } from '../lib/queryClient';
  import { fetchPublicacaoPagesForObjeto, fetchPublicacaoList } from '../lib/pncpPublicacao';
  import { archivedContratoToInternalContract, type PNCPContract } from '../lib/pncp';
  import { queryParquetFallback, archiveErrorMessage } from '../lib/parquetFallback';
  import type { ArchivedContrato } from '../lib/archive/schema';
  import { resolve } from '../lib/baseUrl';
  import { formatBRL, formatDate } from '../lib/format';
  import EmptyState from './EmptyState.svelte';
  import AlertBanner from './AlertBanner.svelte';
  import CopyButton from './CopyButton.svelte';
  import ShareButton from './ShareButton.svelte';
  import { setPageMeta } from '../lib/pageMeta';
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
        // back into PNCPContract for a uniform render path. Only `empty`
        // is treated as a valid no-results state — other archive failures
        // (no_manifest, duckdb_init_failed, sql_error, timeout) must
        // throw so the query does not succeed, the user sees the real
        // error, and lastVisited is *not* advanced past contracts that
        // we never actually checked.
        const archived = await queryParquetFallback<ArchivedContrato>(
          'ni_fornecedor',
          e.filter,
          50,
          'data_publicacao_pncp',
        );
        if (archived.ok) return archived.rows.map((row) => archivedContratoToInternalContract(row));
        if (archived.reason === 'empty') return [];
        throw new Error(archiveErrorMessage(archived.reason));
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

  $effect(() => {
    if (!entry) return;
    setPageMeta({
      title: `Vigilância: ${entry.label}`,
      description: `Acompanhamento das novidades para "${entry.label}".`,
    });
  });
</script>

<section>
  {#if mounted && !entry}
    <EmptyState
      title="Vigilância não encontrada"
      message="Esta vigilância não está salva neste navegador."
    />
  {:else if entry}
    <header>
      <hgroup>
        <p class="eyebrow">🔔 Vigilância</p>
        <h1>{entry.label}</h1>
        <p>
          <code>{entry.filter}</code>
          <CopyButton text={entry.filter} label="Copiar filtro" />
          <ShareButton title={`Vigilância: ${entry.label}`} text={entry.filter} />
        </p>
      </hgroup>
    </header>

    {#if previousVisited}
      <section
        data-testid="watch-diff-section"
        aria-labelledby="watch-diff-title"
      >
        <h2 id="watch-diff-title">Novidades desde sua última visita</h2>
        {#if newSinceVisit.length === 0}
          <p><small>Nenhuma novidade desde {formatDate(previousVisited)}.</small></p>
        {:else}
          <ul>
            {#each newSinceVisit as c (c.numeroControlePNCP)}
              <li data-testid="watch-diff-item">
                <a href={linkFor(c)}>
                  <strong>{c.objetoContratacao}</strong>
                  <small>{formatDate(c.dataPublicacaoPncp ?? '')}</small>
                  <strong>{formatBRL(c.valorTotalEstimado ?? null)}</strong>
                </a>
              </li>
            {/each}
          </ul>
        {/if}
      </section>
    {/if}

    <section aria-labelledby="watch-all-title">
      <h2 id="watch-all-title">Todos os resultados</h2>
      {#if loading}
        <p aria-busy="true">Buscando…</p>
      {:else if error}
        <AlertBanner title="Não foi possível buscar" message={error.message} level="error" />
      {:else if results.length === 0}
        <EmptyState title="Sem resultados" message="A vigilância ainda não tem correspondências." />
      {:else}
        <ul data-testid="watch-all-results">
          {#each results as c (c.numeroControlePNCP)}
            <li>
              <a href={linkFor(c)}>
                <strong>{c.objetoContratacao}</strong>
                <small>{formatDate(c.dataPublicacaoPncp ?? '')}</small>
              </a>
            </li>
          {/each}
        </ul>
      {/if}
    </section>
  {/if}
</section>

