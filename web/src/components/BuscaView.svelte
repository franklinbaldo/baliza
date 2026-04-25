<script lang="ts">
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { getQueryClient } from '../lib/queryClient';
  import { QUERY_KEYS } from '../lib/queryKeys';
  import { fetchPublicacaoPagesForObjeto } from '../lib/pncpPublicacao';
  import type { PNCPContract } from '../lib/pncp';
  import { isPncpId } from '../lib/pncpId';
  import { resolve } from '../lib/baseUrl';
  import { formatBRL, formatDate } from '../lib/format';
  import AlertBanner from './AlertBanner.svelte';
  import EmptyState from './EmptyState.svelte';

  setQueryClientContext(getQueryClient());

  const { q: qProp = '' }: { q?: string } = $props();

  function initialQ(): string {
    if (qProp) return qProp;
    if (typeof window === 'undefined') return '';
    return new URLSearchParams(window.location.search).get('q') ?? '';
  }

  let searchInput = $state(initialQ());
  let submittedQ = $state(initialQ());

  const query = createQuery(() => {
    // Capture the term up front so a later submit can't mutate the closure
    // mid-flight and bind the result of one fetch to a different cache key.
    const term = submittedQ;
    return {
      queryKey: QUERY_KEYS.busca(term),
      enabled: term.length >= 3 && !isPncpId(term),
      queryFn: () => fetchPublicacaoPagesForObjeto(term),
    };
  });

  const results = $derived(query.data ?? []);
  const loading = $derived(query.isFetching);
  const error = $derived(query.error as Error | null);

  function handleSubmit(ev: Event) {
    ev.preventDefault();
    const term = searchInput.trim();
    // PNCP-ID shortcut: skip the search entirely and jump to the permalink.
    // encodeURIComponent preserves the slash as-is since the detail view
    // matches the raw id, not a URL-encoded variant.
    if (isPncpId(term)) {
      if (typeof window !== 'undefined') {
        window.location.assign(resolve(`contratacao?id=${term}`));
      }
      return;
    }
    submittedQ = term;
    if (typeof window === 'undefined') return;
    // The scenario locks %20-encoded spaces; URLSearchParams.toString() emits
    // `+` for spaces, so build the query string via encodeURIComponent.
    const qs = term ? `?q=${encodeURIComponent(term)}` : '';
    window.history.replaceState(
      {},
      '',
      qs ? `${window.location.pathname}${qs}` : window.location.pathname,
    );
  }

  function truncate(s: string | null | undefined, n: number): string {
    if (!s) return '';
    return s.length > n ? `${s.slice(0, n)}…` : s;
  }

  function linkFor(c: PNCPContract): string {
    return resolve(`contratacao?id=${c.numeroControlePNCP ?? ''}`);
  }
</script>

<div class="busca container">
  <header class="hub-header">
    <span class="type-badge">🔎 BUSCA LIVRE</span>
    <h1>Busca livre no PNCP</h1>
    <p class="meta-row">
      Busca texto-livre nas contratações publicadas no PNCP nos últimos 365 dias.
      Informe também um número de controle PNCP (<code>cnpj-tipo-sequencial/ano</code>)
      para ir direto ao contrato.
    </p>
  </header>

  <form class="search-form" onsubmit={handleSubmit}>
    <label class="sr-only" for="busca-input">Termo a pesquisar</label>
    <input
      id="busca-input"
      type="search"
      bind:value={searchInput}
      placeholder="Ex.: hospital municipal, merenda escolar, 00000000000191-1-000001/2024…"
      aria-label="Termo a pesquisar"
    />
    <button type="submit" class="btn btn-primary">Buscar</button>
  </form>

  {#if !submittedQ || submittedQ.length < 3}
    <EmptyState
      title="Digite o termo a pesquisar"
      message="Use ao menos 3 caracteres para buscar por objeto da contratação."
    />
  {:else if loading}
    <div class="skeleton-wrap" aria-busy="true" aria-label="Buscando contratações">
      <div class="skeleton skeleton-bid"></div>
      <div class="skeleton skeleton-bid"></div>
    </div>
  {:else if error}
    <AlertBanner title="Não foi possível buscar" message={error.message} level="error" />
  {:else if results.length === 0}
    <EmptyState
      title="Nenhuma contratação encontrada"
      message="Nenhuma publicação recente do PNCP corresponde ao termo pesquisado."
    />
  {:else}
    <ul role="listbox" class="busca-results" data-testid="busca-results">
      {#each results as c (c.numeroControlePNCP)}
        <li role="option" aria-selected="false" data-testid="busca-result-item">
          <a href={linkFor(c)} class="result-card">
            <div class="result-head">
              <span class="agency">{c.orgaoEntidade?.razaoSocial ?? 'Órgão'}</span>
              <span class="modality">{c.modalidadeNome ?? ''}</span>
            </div>
            <p class="objeto">{truncate(c.objetoContratacao, 180)}</p>
            <div class="result-foot">
              <span class="date">{formatDate(c.dataPublicacaoPncp ?? '')}</span>
              <span class="valor">{formatBRL(c.valorTotalEstimado ?? null)}</span>
            </div>
          </a>
        </li>
      {/each}
    </ul>
  {/if}
</div>

<style>
  .busca { padding: var(--space-2xl) 0; }
  .hub-header { margin-bottom: var(--space-lg); border-bottom: 2px solid var(--color-base-300); padding-bottom: var(--space-md); }
  .type-badge { font-family: var(--font-mono); font-size: 0.7rem; background: var(--color-primary); color: white; padding: 2px 8px; border-radius: 4px; }
  h1 { font-size: var(--font-size-2xl); margin-top: var(--space-sm); }
  .meta-row { color: var(--color-secondary); font-size: var(--font-size-sm); margin: 4px 0 0; }
  .meta-row code { font-family: var(--font-mono); font-size: 0.85em; }

  .search-form { display: flex; gap: var(--space-sm); margin-bottom: var(--space-lg); }
  .search-form input {
    flex: 1;
    padding: var(--space-sm) var(--space-md);
    border: 1px solid var(--color-base-300);
    border-radius: var(--radius-sm);
    font-size: var(--font-size-md);
  }
  .sr-only {
    position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px;
    overflow: hidden; clip: rect(0, 0, 0, 0); white-space: nowrap; border: 0;
  }

  .skeleton-wrap { display: grid; gap: var(--space-md); }
  .skeleton-bid { height: 5rem; border-radius: var(--radius-sm); }

  .busca-results { list-style: none; margin: 0; padding: 0; display: grid; gap: var(--space-md); }
  .result-card {
    display: block;
    text-decoration: none;
    color: inherit;
    background: var(--color-base-100);
    padding: var(--space-md);
    border: 1px solid var(--color-base-300);
    transition: transform 0.2s, border-color 0.2s, box-shadow 0.2s;
  }
  .result-card:hover, .result-card:focus-visible {
    transform: translate(-2px, -2px);
    border-color: var(--color-primary);
    box-shadow: 4px 4px 0 var(--color-primary);
  }
  .result-head { display: flex; justify-content: space-between; align-items: baseline; gap: var(--space-sm); font-size: var(--font-size-sm); margin-bottom: var(--space-sm); }
  .agency { font-weight: 700; }
  .modality { color: var(--color-secondary); font-size: var(--font-size-xs); }
  .objeto { font-size: var(--font-size-sm); line-height: 1.5; margin-bottom: var(--space-sm); }
  .result-foot { display: flex; justify-content: space-between; align-items: baseline; gap: var(--space-sm); font-size: var(--font-size-sm); }
  .date { font-family: var(--font-mono); color: var(--color-secondary); }
  .valor { font-weight: 800; color: var(--color-primary); }
</style>
