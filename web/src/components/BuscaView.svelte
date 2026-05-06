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
  import HubHeader from './HubHeader.svelte';
  import Skeleton from './Skeleton.svelte';
  import PaginatedList from './PaginatedList.svelte';
  import CopyButton from './CopyButton.svelte';
  import ShareButton from './ShareButton.svelte';
  import { addWatch, isWatched, hydrateWatches } from '../lib/watchStore.svelte';
  import { replaceUrlParams } from '../lib/urlState';

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

  // Post-fetch (visible) filters — distinct from FILTERS server-side keys.
  // `ufFiltro` lives in FILTERS as the PNCP-side filter, leaving `uf` free
  // to mean "narrow what's already on screen".
  function readPostFilters(): { uf: string; modalidade: string } {
    if (typeof window === 'undefined') return { uf: '', modalidade: '' };
    const params = new URLSearchParams(window.location.search);
    return {
      uf: params.get('uf') ?? '',
      modalidade: params.get('modalidade') ?? '',
    };
  }

  const initial = readUrl();
  const initialPost = readPostFilters();

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

  let selectedUf = $state(initialPost.uf);
  let selectedModality = $state(initialPost.modalidade);

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
  function syncUrl(q: string, filters: FilterValues, post?: { uf: string; modalidade: string }): void {
    const entries: Record<string, string> = { ...toUrlEntries(filters) };
    if (q) entries.q = q;
    if (post?.uf) entries.uf = post.uf;
    if (post?.modalidade) entries.modalidade = post.modalidade;
    replaceUrlParams(entries, { merge: false });
  }

  // Track post-fetch filter changes back into the URL so a deep link
  // restores exactly what the user last saw — including UF/modalidade
  // narrowing applied after the PNCP fetch.
  $effect(() => {
    const uf = selectedUf;
    const mod = selectedModality;
    syncUrl(submitted.q, submitted.filters, { uf, modalidade: mod });
  });

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
  function exportCsv(): void {
    const slug = (submitted.q || 'busca').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '') || 'busca';
    exporters.downloadTextFile(`baliza-${slug}.csv`, exporters.toCsv(filteredResults), 'text/csv');
  }

  const markdown = $derived(exporters.toMarkdown(filteredResults));
  const shareText = $derived(
    submitted.q
      ? `Busca "${submitted.q}" no Baliza — ${aggregates.count} contratações`
      : `Busca no Baliza — ${aggregates.count} contratações`,
  );
</script>

<section>
  <HubHeader kicker="🔎 Busca livre" title="Busca livre no PNCP">
    {#snippet lede()}
      <p>
        Busca texto-livre nas contratações publicadas no PNCP nos últimos 365 dias.
        Informe também um número de controle PNCP (<code>cnpj-tipo-sequencial/ano</code>)
        para ir direto ao contrato.
      </p>
    {/snippet}
  </HubHeader>

  <form role="search" onsubmit={handleSubmit}>
    <label for="busca-input" class="sr-only">Termo a pesquisar</label>
    <div role="group" aria-label="Buscar contratações">
      <input
        id="busca-input"
        type="search"
        bind:value={searchInput}
        placeholder="Ex.: hospital municipal, merenda escolar, 00000000000191-1-000001/2024…"
        aria-label="Termo a pesquisar"
      />
      <button type="submit">Buscar</button>
    </div>

    <details open={draftHasAny}>
      <summary>Filtros Avançados (API do PNCP)</summary>
      <fieldset class="grid">
        <legend class="sr-only">Filtros avançados de busca no PNCP</legend>
        {#each Object.values(FILTERS) as def (def.key)}
          <label>
            {def.label}
            <input type="text" bind:value={drafts[def.key]} placeholder={def.placeholder} />
          </label>
        {/each}
      </fieldset>
    </details>
  </form>

  {#if redirecting || (!submitted.q && !hasAnyFilter(submitted.filters)) || (!hasAnyFilter(submitted.filters) && submitted.q.length < 3)}
    <EmptyState
      title="Busca de Contratações"
      message="Use ao menos 3 caracteres no termo ou preencha um filtro avançado para buscar."
    />
  {:else if loading}
    <Skeleton label="Buscando contratações" rows={4} />
  {:else if error}
    <AlertBanner title="Não foi possível buscar" message={error.message} level="error" />
  {:else if results.length === 0}
    <EmptyState
      title="Nenhuma contratação encontrada"
      message="Nenhuma publicação recente do PNCP corresponde ao termo pesquisado."
    />
    {#if suggestion}
      <p>
        Você quis dizer:
        <button
          type="button"
          data-testid="busca-suggestion"
          onclick={applySuggestion}
        >{suggestion}</button>?
      </p>
    {/if}
  {:else}
    {@render searchResultsUI()}
  {/if}
</section>

{#snippet searchResultsUI()}
  <fieldset data-testid="busca-filters" class="grid">
    <legend class="sr-only">Refinar resultados visíveis</legend>
    <label>
      UF
      <select bind:value={selectedUf} aria-label="Filtrar por UF" data-testid="busca-filter-uf">
        <option value="">Todas</option>
        {#each ufOptions as uf (uf)}<option value={uf}>{uf}</option>{/each}
      </select>
    </label>
    <label>
      Modalidade
      <select bind:value={selectedModality} aria-label="Filtrar por modalidade" data-testid="busca-filter-modality">
        <option value="">Todas</option>
        {#each modalityOptions as mod (mod)}<option value={mod}>{mod}</option>{/each}
      </select>
    </label>
  </fieldset>

  <div role="group" aria-label="Exportar resultados visíveis" class="actions">
    <button type="button" class="outline" data-testid="busca-save-watch" onclick={saveWatch} aria-pressed={queryWatched} disabled={!watchKey || results.length === 0}>
      {queryWatched ? '✓ Vigilância salva' : 'Salvar vigilância'}
    </button>
    <button type="button" class="outline" data-testid="busca-export-csv" onclick={exportCsv}>Exportar CSV</button>
    <CopyButton text={markdown} variant="inline" label="Exportar Markdown" testid="busca-export-markdown" />
    <ShareButton title="Busca Baliza" text={shareText} variant="inline" testid="busca-share" />
  </div>

  <article data-testid="busca-aggregates" aria-label="Resumo dos resultados visíveis">
    <dl class="grid">
      <div><dt><small>Contratos</small></dt><dd data-testid="busca-aggregate-count"><strong>{aggregates.count}</strong></dd></div>
      <div><dt><small>Valor total</small></dt><dd data-testid="busca-aggregate-total"><strong>{formatBRL(aggregates.total)}</strong></dd></div>
      <div><dt><small>Valor médio</small></dt><dd data-testid="busca-aggregate-average"><strong>{formatBRL(aggregates.average)}</strong></dd></div>
    </dl>
  </article>

  {#if filteredResults.length === 0}
    <EmptyState
      title="Nenhum resultado para os filtros selecionados"
      message="Ajuste a UF ou a modalidade para ver mais contratações."
    />
  {:else}
    <PaginatedList items={filteredResults} pageSize={20} resetTrigger={`${submitted.q}${selectedUf}${selectedModality}`}>
      {#snippet children(pageItems)}
        <ul role="listbox" data-testid="busca-results">
          {#each pageItems as c (c.numeroControlePNCP)}
            <li role="option" aria-selected="false" data-testid="busca-result-item">
              <article>
                <a href={linkFor(c)}>
                  <header>
                    <strong>{c.orgaoEntidade?.razaoSocial ?? 'Órgão'}</strong>
                    <small data-badge>{c.modalidadeNome ?? ''}</small>
                  </header>
                  <p title={c.objetoContratacao}>{truncate(c.objetoContratacao, 180)}</p>
                  <footer>
                    <small>{formatDate(c.dataPublicacaoPncp ?? '')}</small>
                    <strong>{formatBRL(c.valorTotalEstimado ?? null)}</strong>
                  </footer>
                </a>
              </article>
            </li>
          {/each}
        </ul>
      {/snippet}
    </PaginatedList>
  {/if}
{/snippet}

