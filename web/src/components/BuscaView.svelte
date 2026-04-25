<script lang="ts">
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { getQueryClient } from '../lib/queryClient';
  import { QUERY_KEYS } from '../lib/queryKeys';
  import { fetchPublicacaoPagesForObjeto } from '../lib/pncpPublicacao';
  import type { PNCPContract } from '../lib/pncp';
  import { isPncpId } from '../lib/pncpId';
  import { resolve } from '../lib/baseUrl';
  import * as nav from '../lib/navigate';
  import * as exporters from '../lib/exporters';
  import { formatBRL, formatDate } from '../lib/format';
  import AlertBanner from './AlertBanner.svelte';
  import EmptyState from './EmptyState.svelte';

  setQueryClientContext(getQueryClient());

  const { q: qProp = '' }: { q?: string } = $props();

  function initialQ(): string {
    if (qProp) return qProp.trim();
    if (typeof window === 'undefined') return '';
    return (new URLSearchParams(window.location.search).get('q') ?? '').trim();
  }

  let searchInput = $state(initialQ());
  let submittedQ = $state(initialQ());

  let selectedUf = $state('');
  let selectedModality = $state('');

  const redirecting = $derived(!!submittedQ && isPncpId(submittedQ));

  $effect(() => {
    if (submittedQ && isPncpId(submittedQ)) {
      nav.navigateReplace(resolve(`contratacao?id=${submittedQ}`));
    }
  });

  const query = createQuery(() => {
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

  function uniqSorted(values: Array<string | null | undefined>): string[] {
    const seen: Record<string, true> = {};
    const out: string[] = [];
    for (const v of values) {
      const s = (v ?? '').trim();
      if (s && !seen[s]) {
        seen[s] = true;
        out.push(s);
      }
    }
    return out.sort((a, b) => a.localeCompare(b, 'pt-BR'));
  }

  const ufOptions = $derived(uniqSorted(results.map((c) => c.unidadeOrgao?.ufSigla)));
  const modalityOptions = $derived(uniqSorted(results.map((c) => c.modalidadeNome)));

  const filteredResults = $derived(
    results.filter((c) => {
      if (selectedUf && c.unidadeOrgao?.ufSigla !== selectedUf) return false;
      if (selectedModality && c.modalidadeNome !== selectedModality) return false;
      return true;
    }),
  );

  const aggregates = $derived.by(() => {
    const count = filteredResults.length;
    // Compute the average over rows that actually carry a numeric estimate
    // — rows without a value would otherwise drag the mean toward 0 and
    // misrepresent what the visible buyers are paying.
    let total = 0;
    let withValue = 0;
    for (const c of filteredResults) {
      if (typeof c.valorTotalEstimado === 'number') {
        total += c.valorTotalEstimado;
        withValue += 1;
      }
    }
    const average = withValue > 0 ? total / withValue : 0;
    return { count, total, average };
  });

  // Accent suggestion is gated behind a zero-result, no-error fetch — i.e.
  // the rare path. Lazy-loading the ~80-entry lexicon keeps it out of the
  // main BuscaView chunk so the common search path doesn't pay for it.
  let suggestion = $state<string | null>(null);
  $effect(() => {
    const term = submittedQ;
    if (
      !term ||
      term.length < 3 ||
      isPncpId(term) ||
      loading ||
      error ||
      results.length > 0
    ) {
      suggestion = null;
      return;
    }
    let cancelled = false;
    import('../lib/accentLexicon').then(({ suggestAccented }) => {
      if (!cancelled) suggestion = suggestAccented(term);
    });
    return () => {
      cancelled = true;
    };
  });

  function applySuggestion(): void {
    if (!suggestion) return;
    searchInput = suggestion;
    submittedQ = suggestion;
    selectedUf = '';
    selectedModality = '';
    if (typeof window === 'undefined') return;
    const qs = `?q=${encodeURIComponent(suggestion)}`;
    window.history.replaceState({}, '', `${window.location.pathname}${qs}`);
  }

  function handleSubmit(ev: Event) {
    ev.preventDefault();
    const term = searchInput.trim();
    if (isPncpId(term)) {
      nav.navigate(resolve(`contratacao?id=${term}`));
      return;
    }
    submittedQ = term;
    selectedUf = '';
    selectedModality = '';
    if (typeof window === 'undefined') return;
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

  // The export buttons act on what the user sees — i.e. filteredResults,
  // not the raw fetch — so the file matches the on-screen aggregate strip.
  let copyStatus = $state<'idle' | 'copied' | 'error'>('idle');
  let copyTimer: ReturnType<typeof setTimeout> | null = null;

  // Clear any pending status reset when the component unmounts so the
  // timer cannot fire a state assignment after destroy.
  $effect(() => {
    return () => {
      if (copyTimer !== null) {
        clearTimeout(copyTimer);
        copyTimer = null;
      }
    };
  });

  function exportCsv(): void {
    const slug = (submittedQ || 'busca').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '') || 'busca';
    exporters.downloadTextFile(`baliza-${slug}.csv`, exporters.toCsv(filteredResults), 'text/csv');
  }

  async function exportMarkdown(): Promise<void> {
    const md = exporters.toMarkdown(filteredResults);
    try {
      await navigator.clipboard.writeText(md);
      copyStatus = 'copied';
    } catch {
      copyStatus = 'error';
    }
    // Cancel any pending reset so rapid clicks do not stack timers.
    if (copyTimer !== null) clearTimeout(copyTimer);
    copyTimer = setTimeout(() => {
      copyStatus = 'idle';
      copyTimer = null;
    }, 2000);
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

  {#if redirecting || !submittedQ || submittedQ.length < 3}
    <EmptyState
      title="Digite o termo a pesquisar"
      message="Use ao menos 3 caracteres para buscar por objeto da contratação."
    />
  {:else if loading}
    <div class="skeleton-wrap" aria-busy="true" aria-label="Buscando contratações">
      <div class="skeleton skeleton-bid"></div>
      <div class="skeleton skeleton-bid"></div>
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
    {#if suggestion}
      <p class="suggestion-row">
        Você quis dizer:
        <button
          type="button"
          class="suggestion-link"
          data-testid="busca-suggestion"
          onclick={applySuggestion}
        >{suggestion}</button>?
      </p>
    {/if}
  {:else}
    <div class="filters" data-testid="busca-filters">
      <label class="filter">
        <span>UF</span>
        <select
          bind:value={selectedUf}
          aria-label="Filtrar por UF"
          data-testid="busca-filter-uf"
        >
          <option value="">Todas</option>
          {#each ufOptions as uf (uf)}
            <option value={uf}>{uf}</option>
          {/each}
        </select>
      </label>
      <label class="filter">
        <span>Modalidade</span>
        <select
          bind:value={selectedModality}
          aria-label="Filtrar por modalidade"
          data-testid="busca-filter-modality"
        >
          <option value="">Todas</option>
          {#each modalityOptions as mod (mod)}
            <option value={mod}>{mod}</option>
          {/each}
        </select>
      </label>
      <div class="export-actions" role="group" aria-label="Exportar resultados visíveis">
        <button
          type="button"
          class="btn btn-secondary"
          data-testid="busca-export-csv"
          onclick={exportCsv}
        >Exportar CSV</button>
        <button
          type="button"
          class="btn btn-secondary"
          data-testid="busca-export-markdown"
          onclick={exportMarkdown}
        >Exportar Markdown</button>
        {#if copyStatus !== 'idle'}
          <span
            class="copy-status"
            role="status"
            aria-live="polite"
            data-testid="busca-export-status"
          >{copyStatus === 'copied' ? 'Copiado!' : 'Falha ao copiar'}</span>
        {/if}
      </div>
    </div>

    <dl class="aggregates" data-testid="busca-aggregates" aria-label="Resumo dos resultados visíveis">
      <div class="agg-cell">
        <dt>Contratos</dt>
        <dd data-testid="busca-aggregate-count">{aggregates.count}</dd>
      </div>
      <div class="agg-cell">
        <dt>Valor total</dt>
        <dd data-testid="busca-aggregate-total">{formatBRL(aggregates.total)}</dd>
      </div>
      <div class="agg-cell">
        <dt>Valor médio</dt>
        <dd data-testid="busca-aggregate-average">{formatBRL(aggregates.average)}</dd>
      </div>
    </dl>

    {#if filteredResults.length === 0}
      <EmptyState
        title="Nenhum resultado para os filtros selecionados"
        message="Ajuste a UF ou a modalidade para ver mais contratações."
      />
    {:else}
      <ul role="listbox" class="busca-results" data-testid="busca-results">
        {#each filteredResults as c (c.numeroControlePNCP)}
          <li role="option" aria-selected="false" data-testid="busca-result-item">
            <a href={linkFor(c)} class="result-card">
              <div class="result-head">
                <span class="agency">{c.orgaoEntidade?.razaoSocial ?? 'Órgão'}</span>
                <span class="modality">{c.modalidadeNome ?? ''}</span>
              </div>
              <p class="objeto" title={c.objetoContratacao}>{truncate(c.objetoContratacao, 180)}</p>
              <div class="result-foot">
                <span class="date">{formatDate(c.dataPublicacaoPncp ?? '')}</span>
                <span class="valor">{formatBRL(c.valorTotalEstimado ?? null)}</span>
              </div>
            </a>
          </li>
        {/each}
      </ul>
    {/if}
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

  .filters {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-md);
    margin-bottom: var(--space-md);
  }
  .filter { display: grid; gap: 4px; font-size: var(--font-size-xs); color: var(--color-secondary); }
  .filter select {
    padding: 6px 8px;
    border: 1px solid var(--color-base-300);
    border-radius: var(--radius-sm);
    background: var(--color-base-100);
    font-size: var(--font-size-sm);
  }
  .export-actions {
    margin-left: auto;
    display: flex;
    align-items: end;
    gap: var(--space-sm);
  }
  .copy-status {
    font-size: var(--font-size-xs);
    color: var(--color-secondary);
    align-self: center;
  }

  .aggregates {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
    gap: var(--space-md);
    margin: 0 0 var(--space-lg);
    padding: var(--space-md);
    background: var(--color-base-100);
    border: 1px solid var(--color-base-300);
    border-radius: var(--radius-sm);
  }
  .agg-cell dt {
    font-size: var(--font-size-xs);
    color: var(--color-secondary);
    text-transform: uppercase;
    letter-spacing: 0.04em;
  }
  .agg-cell dd { margin: 4px 0 0; font-weight: 700; font-size: var(--font-size-md); }

  .suggestion-row { margin-top: var(--space-md); font-size: var(--font-size-sm); }
  .suggestion-link {
    background: none;
    border: 0;
    padding: 0;
    color: var(--color-primary);
    font: inherit;
    cursor: pointer;
    text-decoration: underline;
  }
  .suggestion-link:hover, .suggestion-link:focus-visible { text-decoration: none; }

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
