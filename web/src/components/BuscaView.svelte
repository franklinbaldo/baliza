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
  import {
    FILTERS,
    parseInput as parseSearchInput,
    readFilters,
    toUrlEntries,
    toApiParams,
    hasAny as hasAnyFilter,
    type FilterValues,
  } from '../lib/searchFilters';
  import AlertBanner from './AlertBanner.svelte';
  import EmptyState from './EmptyState.svelte';
  import PaginatedList from './PaginatedList.svelte';
  import { addWatch, isWatched, hydrateWatches } from '../lib/watchStore.svelte';

  setQueryClientContext(getQueryClient());

  const { q: qProp = '' }: { q?: string } = $props();

  // Hydrate from URL: every recognised filter (`?ibge=`, `?cnpj=`, `?uf=`,
  // …) is read as its own param; `?q=` is also scanned for inline tags
  // so power-user shorthand (`?q=ibge:1100205+merenda`) still works on
  // load. URL is the single source of truth — state is derived from it
  // on init and pushed back on submit.
  function readUrl(): { input: string; submitted: { q: string; filters: FilterValues } } {
    if (typeof window === 'undefined') {
      const parsed = parseSearchInput(qProp.trim());
      return {
        input: qProp.trim(),
        submitted: { q: parsed.term, filters: parsed.filters },
      };
    }
    const params = new URLSearchParams(window.location.search);
    const rawQ = (qProp || params.get('q') || '').trim();
    const parsed = parseSearchInput(rawQ);
    // URL params take precedence over inline tags for the same key — a
    // shareable link with explicit `?ibge=` shouldn't be silently
    // overridden by a stale tag inside the q string.
    const filters = { ...parsed.filters, ...readFilters(params) };
    return { input: rawQ, submitted: { q: parsed.term, filters } };
  }

  const initial = readUrl();

  // searchInput is what's bound to the visible <input>. We never rewrite
  // it during submit — that previously caused a "snap" as the field was
  // re-canonicalised under the user's cursor. Inline tags stay as the
  // user typed them; the parsed form is held separately in `submitted`.
  let searchInput = $state(initial.input);

  // Submitted = the snapshot the data layer queries against. Replaced
  // atomically on submit so query keys move in lockstep.
  let submitted = $state<{ q: string; filters: FilterValues }>(initial.submitted);

  // Drafts back the per-filter inputs in the "Filtros Avançados" drawer.
  // Editing a draft doesn't refetch — the user has to click Buscar so
  // their typing isn't shipped to PNCP on every keystroke. Initialised
  // from URL so the drawer reflects what's actually applied.
  const drafts: Record<string, string> = $state(
    Object.fromEntries(
      Object.values(FILTERS).map((def) => [def.key, initial.submitted.filters[def.key] ?? '']),
    ),
  );
  const draftHasAny = $derived(Object.values(drafts).some((v) => !!v));

  let selectedUf = $state('');
  let selectedModality = $state('');

  const redirecting = $derived(!!submitted.q && isPncpId(submitted.q));

  $effect(() => {
    if (submitted.q && isPncpId(submitted.q)) {
      nav.navigateReplace(resolve(`contratacao?id=${submitted.q}`));
    }
  });

  const query = createQuery(() => {
    const { q: term, filters } = submitted;
    const hasAdvanced = hasAnyFilter(filters);
    return {
      // Query key still surfaces ibge/cnpj as primary keys for cache
      // continuity with the previous shape; additional filters fold into
      // the suffix object so adding one in the registry doesn't require
      // a queryKeys.ts change.
      queryKey: QUERY_KEYS.busca(term, filters.cnpj ?? '', filters.ibge ?? '', filters),
      enabled: (term.length >= 3 && !isPncpId(term)) || hasAdvanced,
      queryFn: () =>
        fetchPublicacaoPagesForObjeto(term, { filters: toApiParams(filters) }),
    };
  });

  const results = $derived(query.data ?? []);
  const loading = $derived(query.isFetching);
  const error = $derived(query.error as Error | null);

  $effect(() => { hydrateWatches(); });

  // Canonical key includes q + all active filters so two watches with the
  // same free-text term but different filters are not collapsed as duplicates.
  // Keys are sorted so cnpj+ibge and ibge+cnpj produce the same string.
  const watchKey = $derived.by(() => {
    const raw: Record<string, string> = {};
    if (submitted.q) raw['q'] = submitted.q;
    for (const [k, v] of Object.entries(toUrlEntries(submitted.filters))) {
      raw[k] = v;
    }
    return new URLSearchParams(Object.keys(raw).sort().map((k) => [k, raw[k]])).toString();
  });
  const queryWatched = $derived(isWatched('query', watchKey));

  function saveWatch() {
    if (!watchKey) return;
    addWatch('query', watchKey, submitted.q || watchKey);
  }

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
    const term = submitted.q;
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

  // Pushes the current (q, filters) pair into the URL using one param per
  // filter — `?q=merenda&ibge=1100205&uf=RO`. Semantic separation makes
  // links easier to share/debug and lets applySuggestion() mutate just
  // `q` without nuking the structured filters.
  function syncUrl(q: string, filters: FilterValues): void {
    if (typeof window === 'undefined') return;
    const entries: Record<string, string> = { ...toUrlEntries(filters) };
    if (q) entries.q = q;
    const qs = new URLSearchParams(entries).toString();
    window.history.replaceState(
      {},
      '',
      qs ? `${window.location.pathname}?${qs}` : window.location.pathname,
    );
  }

  function applySuggestion(): void {
    if (!suggestion) return;
    // Update the visible input to mirror the substitution; preserve
    // structured filters by only replacing the q half of `submitted`.
    searchInput = suggestion;
    submitted = { q: suggestion, filters: submitted.filters };
    selectedUf = '';
    selectedModality = '';
    syncUrl(suggestion, submitted.filters);
  }

  function handleSubmit(ev: Event) {
    ev.preventDefault();
    const parsed = parseSearchInput(searchInput);

    if (isPncpId(parsed.term)) {
      nav.navigate(resolve(`contratacao?id=${parsed.term}`));
      return;
    }

    // Merge: drafts seed the result, inline tags in the q-field win on
    // overlap. That matches user intent — typing `cnpj:1234…` in the
    // main box should take precedence over a stale draft value left in
    // the drawer. Drafts are passed through `normalise` so a typo
    // ("ibge:42") is dropped here too rather than emitted to the URL.
    const filters: Record<string, string> = {};
    for (const def of Object.values(FILTERS)) {
      const draft = drafts[def.key];
      if (draft) {
        const n = def.normalise(draft);
        if (n) filters[def.key] = n;
      }
    }
    Object.assign(filters, parsed.filters);

    // Reflect canonical values back into the drawer drafts (e.g. "sp"
    // becomes "SP", "12.345.678/0001-90" loses punctuation) so the user
    // sees what was actually applied.
    for (const def of Object.values(FILTERS)) {
      drafts[def.key] = filters[def.key] ?? '';
    }

    submitted = { q: parsed.term, filters };
    selectedUf = '';
    selectedModality = '';
    syncUrl(parsed.term, filters);
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
    const slug = (submitted.q || 'busca').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '') || 'busca';
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
    <div class="search-main">
      <label class="sr-only" for="busca-input">Termo a pesquisar</label>
      <input
        id="busca-input"
        type="search"
        bind:value={searchInput}
        placeholder="Ex.: hospital municipal, merenda escolar, 00000000000191-1-000001/2024…"
        aria-label="Termo a pesquisar"
      />
      <button type="submit" class="btn btn-primary">Buscar</button>
    </div>
    
    <details class="advanced-filters" open={draftHasAny}>
      <summary>Filtros Avançados (API do PNCP)</summary>
      <div class="advanced-grid">
        {#each Object.values(FILTERS) as def (def.key)}
          <label>
            <span>{def.label}</span>
            <input type="text" bind:value={drafts[def.key]} placeholder={def.placeholder} />
          </label>
        {/each}
      </div>
    </details>
  </form>

  {#if redirecting || (!submitted.q && !hasAnyFilter(submitted.filters)) || (!hasAnyFilter(submitted.filters) && submitted.q.length < 3)}
    <EmptyState
      title="Busca de Contratações"
      message="Use ao menos 3 caracteres no termo ou preencha um filtro avançado para buscar."
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
          data-testid="busca-save-watch"
          onclick={saveWatch}
          aria-pressed={queryWatched}
          disabled={!watchKey || results.length === 0}
        >{queryWatched ? '✓ Vigilância salva' : 'Salvar vigilância'}</button>
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
      <PaginatedList items={filteredResults} pageSize={20} resetTrigger={`${submitted.q}${selectedUf}${selectedModality}`}>
        {#snippet children(pageItems)}
          <ul role="listbox" class="busca-results" data-testid="busca-results">
            {#each pageItems as c (c.numeroControlePNCP)}
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
        {/snippet}
      </PaginatedList>
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

  .search-form { display: grid; gap: var(--space-sm); margin-bottom: var(--space-lg); }
  .search-main { display: flex; gap: var(--space-sm); }
  .search-main input {
    flex: 1;
    padding: var(--space-sm) var(--space-md);
    border: 1px solid var(--color-base-300);
    border-radius: var(--radius-sm);
    font-size: var(--font-size-md);
  }
  .advanced-filters {
    font-size: var(--font-size-sm);
    color: var(--color-secondary);
  }
  .advanced-filters summary {
    cursor: pointer;
    user-select: none;
    margin-bottom: var(--space-sm);
    display: inline-block;
  }
  .advanced-filters summary:hover { color: var(--color-primary); }
  .advanced-grid {
    display: flex;
    gap: var(--space-md);
    flex-wrap: wrap;
    background: var(--color-base-100);
    padding: var(--space-md);
    border: 1px solid var(--color-base-300);
    border-radius: var(--radius-sm);
    margin-top: var(--space-xs);
  }
  .advanced-grid label { display: grid; gap: 6px; flex: 1; min-width: 200px; }
  .advanced-grid input {
    padding: 8px 10px;
    border: 1px solid var(--color-base-300);
    border-radius: var(--radius-sm);
    font-size: var(--font-size-sm);
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
