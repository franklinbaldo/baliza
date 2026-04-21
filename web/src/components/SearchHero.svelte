<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import { z } from 'zod';
  import { PROJECT_MISSION, SEARCH_HINTS } from '../lib/homepage-content';
  import { isPncpId, parsePncpId } from '../lib/pncpId';
  import { formatBRL, formatDate, normalizeSearchInput } from '../lib/format';
  import EmptyState from './EmptyState.svelte';
  import AlertBanner from './AlertBanner.svelte';

  // FRONTEND.md requires Zod validation at every external data boundary. The
  // PNCP /api/search envelope carries a few fields we render directly; the
  // schema tolerates unknown extras via passthrough() so PNCP adding fields
  // does not break us.
  const SearchItemSchema = z
    .object({
      numero_controle_pncp: z.string().optional(),
      title: z.string().optional(),
      description: z.string().optional(),
      orgao_nome: z.string().optional(),
      data_publicacao_pncp: z.string().optional(),
      valor_global: z.number().nullable().optional(),
      uf_sigla: z.string().nullable().optional(),
      modalidade_nome: z.string().nullable().optional(),
    })
    .passthrough();

  const PncpSearchResponseSchema = z.object({
    items: z.array(SearchItemSchema).default([]),
    total: z.number().optional(),
  });

  // PNCP's public consulta API (Swagger: https://pncp.gov.br/api/consulta/swagger-ui/index.html)
  // has no free-text parameter for /v1/contratacoes/publicacao. The portal's user-facing
  // search is backed by /api/search which does support keyword queries, so we use it here.
  // Endpoint: https://pncp.gov.br/api/search?q=<query>&tipos_documento=edital&pagina=1
  // Response envelope: { items: [...], total: number }
  const SEARCH_ENDPOINT = 'https://pncp.gov.br/api/search';
  const DEBOUNCE_MS = 300;

  interface SearchItem {
    numero_controle_pncp?: string;
    title?: string;
    description?: string;
    orgao_nome?: string;
    data_publicacao_pncp?: string;
    valor_global?: number | null;
    // PNCP free-text search occasionally echoes UF and modality in the item
    // envelope. Whenever present, the aggregate-filter strip lets the user
    // narrow by those dimensions without a round-trip to the API.
    uf_sigla?: string | null;
    modalidade_nome?: string | null;
  }

  let query = $state('');
  const cleanQuery = $derived(normalizeSearchInput(query));
  const digitsOnly = $derived(cleanQuery.replace(/\D/g, ''));
  const isCnpj     = $derived(/^\d{14}$/.test(digitsOnly) && /^\d{14}$/.test(cleanQuery));
  const isIbge     = $derived(/^\d{7}$/.test(cleanQuery));
  const isContract = $derived(isPncpId(cleanQuery));
  const hasPattern = $derived(isCnpj || isIbge || isContract);

  function navigateFor(value: string): string | null {
    if (/^\d{14}$/.test(value)) return `/baliza/orgao?cnpj=${value}`;
    if (/^\d{7}$/.test(value))  return `/baliza/municipio?ibge=${value}`;
    if (parsePncpId(value))     return `/baliza/contratacao?id=${value}`;
    return null;
  }

  let results = $state<SearchItem[]>([]);
  let searching = $state(false);
  let searchError = $state<string | null>(null);
  let didSearch = $state(false);
  let lastSearchedTerm = $state('');
  let ufFilter = $state<string>('');
  let modalidadeFilter = $state<string>('');
  let debounceTimer: ReturnType<typeof setTimeout> | null = null;
  let searchToken = 0;

  function slugifyFilename(raw: string): string {
    return raw
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '')
      .slice(0, 64);
  }

  const ufOptions = $derived(
    Array.from(
      new Set(
        results
          .map((r) => (r.uf_sigla ?? '').toString().trim())
          .filter((uf) => uf.length > 0),
      ),
    ).sort(),
  );

  const modalidadeOptions = $derived(
    Array.from(
      new Set(
        results
          .map((r) => (r.modalidade_nome ?? '').toString().trim())
          .filter((m) => m.length > 0),
      ),
    ).sort(),
  );

  const filteredResults = $derived(
    results.filter((r) => {
      if (ufFilter && (r.uf_sigla ?? '') !== ufFilter) return false;
      if (modalidadeFilter && (r.modalidade_nome ?? '') !== modalidadeFilter)
        return false;
      return true;
    }),
  );

  const aggregates = $derived.by(() => {
    const count = filteredResults.length;
    const total = filteredResults.reduce(
      (acc, r) => acc + (typeof r.valor_global === 'number' ? r.valor_global : 0),
      0,
    );
    const average = count > 0 ? total / count : 0;
    return { count, total, average };
  });

  function truncate(s: string, n: number) {
    if (!s) return '';
    return s.length > n ? s.slice(0, n - 1) + '…' : s;
  }

  function pushQueryToUrl(term: string) {
    if (typeof window === 'undefined') return;
    // eslint-disable-next-line svelte/prefer-svelte-reactivity
    const params = new URLSearchParams(window.location.search);
    if (term) {
      params.set('q', term);
    } else {
      params.delete('q');
    }
    const next = params.toString();
    const url = next ? `${window.location.pathname}?${next}` : window.location.pathname;
    window.history.replaceState({}, '', url);
  }

  async function runSearch(term: string) {
    const token = ++searchToken;
    searching = true;
    searchError = null;
    lastSearchedTerm = term;
    pushQueryToUrl(term);
    try {
      const url = `${SEARCH_ENDPOINT}?q=${encodeURIComponent(term)}&tipos_documento=edital&pagina=1`;
      const res = await fetch(url);
      if (!res.ok) throw new Error('Falha ao consultar o PNCP. Tente novamente em instantes.');
      const parsed = PncpSearchResponseSchema.safeParse(await res.json());
      if (!parsed.success) throw new Error('Resposta inesperada do PNCP.');
      if (token !== searchToken) return;
      results = parsed.data.items.slice(0, 10);
      didSearch = true;
      ufFilter = '';
      modalidadeFilter = '';
    } catch (err) {
      if (token !== searchToken) return;
      results = [];
      didSearch = true;
      searchError = err instanceof Error ? err.message : 'Erro na busca PNCP.';
    } finally {
      if (token === searchToken) searching = false;
    }
  }

  function isShapeMatch(term: string) {
    return /^\d{14}$/.test(term) || /^\d{7}$/.test(term) || isPncpId(term);
  }

  function scheduleSearch(term: string) {
    if (debounceTimer) clearTimeout(debounceTimer);
    if (term.length < 3 || isShapeMatch(term)) {
      results = [];
      searchError = null;
      didSearch = false;
      searching = false;
      // Keep URL in sync on shape-match or cleared state so reload does not
      // replay a query the user already dismissed.
      pushQueryToUrl(term);
      return;
    }
    debounceTimer = setTimeout(() => runSearch(term), DEBOUNCE_MS);
  }

  onMount(() => {
    if (typeof window === 'undefined') return;
    const initial = new URLSearchParams(window.location.search).get('q') ?? '';
    if (initial) {
      query = initial;
      scheduleSearch(normalizeSearchInput(initial));
    }
  });

  onDestroy(() => {
    if (debounceTimer) clearTimeout(debounceTimer);
    searchToken++;
  });

  function handleInput(e: Event) {
    const raw = (e.target as HTMLInputElement).value;
    query = raw;
    scheduleSearch(normalizeSearchInput(raw));
  }

  function handleSubmit(e: Event) {
    e.preventDefault();
    const target = navigateFor(cleanQuery);
    if (target && typeof window !== 'undefined') {
      window.location.assign(target);
      return;
    }
    if (cleanQuery.length >= 3 && !hasPattern) {
      if (debounceTimer) clearTimeout(debounceTimer);
      runSearch(cleanQuery);
    }
  }

  // Markdown export is the journalist's counterpart to CSV: the clipboard
  // payload is a ready-to-paste GFM table with one row per visible result.
  function mdEscape(value: unknown): string {
    const raw = value == null ? '' : String(value);
    return raw.replace(/\|/g, '\\|').replace(/\n/g, ' ').trim();
  }

  function buildMarkdownTable(): string {
    const headers = ['PNCP', 'Objeto', 'Órgão', 'UF', 'Modalidade', 'Data', 'Valor'];
    const rows = filteredResults.map((r) =>
      [
        r.numero_controle_pncp
          ? `[${r.numero_controle_pncp}](/baliza/contratacao?id=${r.numero_controle_pncp})`
          : '',
        mdEscape(r.description ?? r.title ?? ''),
        mdEscape(r.orgao_nome ?? ''),
        mdEscape(r.uf_sigla ?? ''),
        mdEscape(r.modalidade_nome ?? ''),
        mdEscape(formatDate(r.data_publicacao_pncp)),
        mdEscape(formatBRL(r.valor_global)),
      ].join(' | '),
    );
    const body = [
      `| ${headers.join(' | ')} |`,
      `| ${headers.map(() => '---').join(' | ')} |`,
      ...rows.map((r) => `| ${r} |`),
    ].join('\n');
    return body;
  }

  let mdCopied = $state(false);
  async function exportMarkdown() {
    if (filteredResults.length === 0) return;
    const md = buildMarkdownTable();
    try {
      await navigator.clipboard.writeText(md);
      mdCopied = true;
      setTimeout(() => {
        mdCopied = false;
      }, 2500);
    } catch {
      // Clipboard API may be blocked (insecure origin, permissions). Fall back
      // to a download so the journalist can still paste the file contents.
      const blob = new Blob([md], { type: 'text/markdown;charset=utf-8' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      const slug = slugifyFilename(lastSearchedTerm) || 'resultados';
      a.href = url;
      a.download = `baliza-busca-${slug}.md`;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    }
  }

  // CSV export of the currently visible (filtered) result set. Columns mirror
  // the on-screen table; values are quoted via RFC 4180 doubling so commas and
  // quotes inside descriptions or org names survive the round-trip.
  function csvEscape(value: unknown): string {
    const raw = value == null ? '' : String(value);
    return `"${raw.replace(/"/g, '""')}"`;
  }

  function exportCsv() {
    if (filteredResults.length === 0) return;
    const headers = [
      'numero_controle_pncp',
      'objeto',
      'orgao',
      'uf',
      'modalidade',
      'data_publicacao_pncp',
      'valor_global',
    ];
    const lines = [headers.join(',')];
    for (const r of filteredResults) {
      lines.push(
        [
          csvEscape(r.numero_controle_pncp ?? ''),
          csvEscape(r.description ?? r.title ?? ''),
          csvEscape(r.orgao_nome ?? ''),
          csvEscape(r.uf_sigla ?? ''),
          csvEscape(r.modalidade_nome ?? ''),
          csvEscape(r.data_publicacao_pncp ?? ''),
          csvEscape(r.valor_global ?? ''),
        ].join(','),
      );
    }
    const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    const slug = slugifyFilename(lastSearchedTerm) || 'resultados';
    a.href = url;
    a.download = `baliza-busca-${slug}.csv`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  }
</script>

<section class="hero-section" id="busca">
  <div class="container hero-inner">
    <h1 class="hero-title">{PROJECT_MISSION.title}</h1>
    <p class="hero-tagline">{PROJECT_MISSION.tagline}</p>

    <form class="search-form search-box" class:detected={hasPattern} onsubmit={handleSubmit}>
      <div class="input-wrapper">
        <label class="input-label" for="search-input">
          <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16" fill="currentColor" class="search-icon" aria-hidden="true">
            <path fill-rule="evenodd" d="M9.965 11.026a5 5 0 1 1 1.06-1.06l2.755 2.754a.75.75 0 1 1-1.06 1.06l-2.755-2.754ZM10.5 7a3.5 3.5 0 1 1-7 0 3.5 3.5 0 0 1 7 0Z" clip-rule="evenodd"/>
          </svg>
          <input
            id="search-input"
            type="text"
            bind:value={query}
            placeholder="CNPJ, código IBGE, ID PNCP ou objeto..."
            oninput={handleInput}
            autocomplete="off"
            spellcheck={false}
            aria-label="Buscar contratos públicos"
          />
          {#if query}
            <button type="button" class="clear-btn" onclick={() => { query = ''; scheduleSearch(''); }} aria-label="Limpar busca">×</button>
          {/if}
          {#if searching}
            <span class="valid-indicator cg-pulse" aria-hidden="true">⏳</span>
          {:else if hasPattern}
            <span class="valid-indicator" aria-hidden="true">✅</span>
          {/if}
        </label>

        <button type="submit" class="search-btn btn btn-primary" disabled={searching}>Buscar</button>
      </div>

      {#if hasPattern}
        <div class="jump-suggestion" role="status" aria-live="polite">
          <span class="jump-label">🎯 Padrão identificado:</span>
          {#if isCnpj}
            <a href={`/baliza/orgao?cnpj=${cleanQuery}`} class="jump-link">
              Explorar Órgão <span class="badge badge-sm badge-info">{cleanQuery}</span>
            </a>
          {:else if isIbge}
            <a href={`/baliza/municipio?ibge=${cleanQuery}`} class="jump-link">
              Explorar Município <span class="badge badge-sm badge-info">{cleanQuery}</span>
            </a>
          {:else if isContract}
            <a href={`/baliza/contratacao?id=${cleanQuery}`} class="jump-link">
              Ver Contratação <span class="badge badge-sm badge-info">#{cleanQuery}</span>
            </a>
          {/if}
        </div>
      {:else if cleanQuery.length >= 3}
        {#if searching}
          <ul class="results-list skeleton-results" aria-busy="true" aria-label="Carregando resultados">
            {#each [1, 2, 3] as _, i (i)}
              <li class="skeleton skeleton-row"></li>
            {/each}
          </ul>
        {:else if searchError}
          <AlertBanner title="Busca PNCP indisponível" message={searchError} level="error" />
        {:else if didSearch && results.length === 0}
          <EmptyState
            title="Nenhum resultado"
            message="A busca PNCP não encontrou contratações para este termo."
          />
        {:else if results.length > 0}
          <div class="result-toolbar" aria-label="Filtros e ações da busca">
            <div class="filters">
              {#if ufOptions.length > 0}
                <label class="filter-field">
                  UF
                  <select bind:value={ufFilter} aria-label="Filtrar por UF">
                    <option value="">Todas</option>
                    {#each ufOptions as uf (uf)}
                      <option value={uf}>{uf}</option>
                    {/each}
                  </select>
                </label>
              {/if}
              {#if modalidadeOptions.length > 0}
                <label class="filter-field">
                  Modalidade
                  <select bind:value={modalidadeFilter} aria-label="Filtrar por modalidade">
                    <option value="">Todas</option>
                    {#each modalidadeOptions as m (m)}
                      <option value={m}>{m}</option>
                    {/each}
                  </select>
                </label>
              {/if}
            </div>
            <dl class="aggregate-strip" data-testid="search-aggregates">
              <div><dt>Contratos</dt><dd>{aggregates.count}</dd></div>
              <div><dt>Valor total</dt><dd>{formatBRL(aggregates.total)}</dd></div>
              <div><dt>Ticket médio</dt><dd>{formatBRL(aggregates.average)}</dd></div>
            </dl>
            <div class="result-actions">
              <button type="button" class="btn btn-outline btn-sm" onclick={exportCsv}>
                Exportar CSV
              </button>
              <button type="button" class="btn btn-outline btn-sm" data-testid="export-markdown" onclick={exportMarkdown}>
                {mdCopied ? 'Markdown copiado ✓' : 'Exportar Markdown'}
              </button>
            </div>
          </div>
          <ul class="results-list" role="listbox" aria-label="Resultados da busca PNCP">
            {#each filteredResults as item (item.numero_controle_pncp)}
              <li role="option" aria-selected="false">
                <a href={`/baliza/contratacao?id=${item.numero_controle_pncp}`} class="result-link">
                  <div class="result-objeto">{truncate(item.description || item.title || '', 120)}</div>
                  <div class="result-meta">
                    <span class="result-orgao">{item.orgao_nome || ''}</span>
                    <span class="result-date">{formatDate(item.data_publicacao_pncp)}</span>
                    <span class="result-valor">{formatBRL(item.valor_global)}</span>
                  </div>
                </a>
              </li>
            {/each}
          </ul>
        {/if}
      {/if}

      <div class="hints" role="status" aria-live="polite">
        <span class="hints-label">Sugestões:</span>
        <div class="hint-chips">
          {#each SEARCH_HINTS as hint (hint)}
            <button type="button" class="hint-chip" onclick={() => { query = hint; scheduleSearch(hint); }}>
              {hint}
            </button>
          {/each}
        </div>
      </div>
    </form>
  </div>
</section>

<style>
  .hero-section {
    padding: var(--space-8) 0 var(--space-6);
  }

  .hero-inner {
    max-width: 800px;
    text-align: center;
    margin: 0 auto;
  }

  .hero-title {
    font-size: var(--text-4xl);
    margin-bottom: var(--space-2);
    color: var(--bulcao-fg);
    line-height: 1.1;
  }

  .hero-tagline {
    color: var(--neutral-500);
    font-size: var(--text-md);
    line-height: 1.6;
    margin-bottom: var(--space-6);
  }

  .search-box {
    background: var(--neutral-0);
    padding: var(--space-4);
    border: 1px solid var(--neutral-200);
    border-radius: var(--radius-0);
    box-shadow: var(--shadow-sm);
  }

  .search-form { display: block; }

  .input-wrapper {
    display: flex;
    align-items: center;
    gap: var(--space-xs);
    position: relative;
  }

  .input-label {
    flex: 1;
    display: flex;
    align-items: center;
    gap: 0.6rem;
    padding: 0.75rem 2.25rem 0.75rem 1rem;
    border: 1px solid var(--neutral-300);
    border-radius: var(--radius-0);
    background: var(--neutral-50);
    transition: border-color var(--duration-fast) var(--ease);
    cursor: text;
    position: relative;
    min-width: 0;
  }

  .input-label:focus-within {
    border-color: var(--bulcao-accent);
  }

  .search-icon {
    width: 1.1rem;
    height: 1.1rem;
    opacity: 0.55;
    flex-shrink: 0;
    color: var(--neutral-500);
  }

  input {
    flex: 1;
    border: none;
    background: transparent;
    outline: none;
    font-size: var(--text-lg);
    color: var(--bulcao-fg);
    font-family: var(--font-body);
  }

  .clear-btn {
    border: none;
    background: transparent;
    font-size: 1.25rem;
    line-height: 1;
    padding: 0 0.25rem;
    cursor: pointer;
    color: var(--color-secondary);
    opacity: 0.6;
  }

  .clear-btn:hover { opacity: 1; }

  .valid-indicator {
    position: absolute;
    right: 0.75rem;
    top: 50%;
    transform: translateY(-50%);
    font-size: 1.2rem;
    line-height: 1;
    pointer-events: none;
  }

  .search-btn {
    flex-shrink: 0;
    font-size: var(--font-size-base);
    border-radius: var(--radius-0);
  }

  @media (max-width: 560px) {
    .input-wrapper {
      flex-direction: column;
      align-items: stretch;
      gap: var(--space-xs);
    }

    .search-btn {
      width: 100%;
    }

    .hints-label {
      flex: 1 0 100%;
    }
  }

  .search-box.detected .input-label {
    border-color: var(--color-primary);
  }

  .jump-suggestion {
    margin-top: var(--space-md);
    padding: var(--space-sm);
    background: var(--color-base-200);
    border: 1px dashed var(--color-primary);
    border-radius: var(--radius-0);
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: var(--space-xs);
  }

  .jump-label {
    font-size: var(--font-size-xs);
    color: var(--color-secondary);
    font-weight: 600;
  }

  .jump-link {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    font-weight: 800;
    color: var(--color-primary);
    text-decoration: none;
    font-family: var(--font-mono);
    font-size: var(--font-size-sm);
  }

  .jump-link:hover { text-decoration: underline; }

  .result-toolbar {
    display: grid;
    grid-template-columns: 1fr auto auto;
    gap: var(--space-md);
    align-items: center;
    margin-top: var(--space-md);
    padding: var(--space-sm);
    background: var(--color-base-200);
    border: 1px solid var(--color-base-300);
    border-radius: var(--radius-0);
    text-align: left;
  }

  @media (max-width: 720px) {
    .result-toolbar {
      grid-template-columns: 1fr;
    }
  }

  .filters {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-sm);
    font-size: var(--font-size-xs);
  }

  .filter-field {
    display: flex;
    flex-direction: column;
    gap: 2px;
    color: var(--color-secondary);
    font-weight: 600;
  }

  .filter-field select {
    background: var(--color-base-100);
    border: 1px solid var(--color-base-300);
    border-radius: var(--radius-0);
    padding: 4px 8px;
    font-size: var(--font-size-xs);
    color: var(--color-base-content);
    font-family: var(--font-sans);
  }

  .aggregate-strip {
    display: flex;
    gap: var(--space-md);
    margin: 0;
    padding: 0;
    font-size: var(--font-size-xs);
    flex-wrap: wrap;
  }

  .aggregate-strip div {
    display: flex;
    flex-direction: column;
    gap: 2px;
  }

  .aggregate-strip dt {
    color: var(--color-secondary);
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    font-size: 0.65rem;
  }

  .aggregate-strip dd {
    margin: 0;
    font-family: var(--font-mono);
    font-weight: 700;
    color: var(--color-primary);
    font-size: var(--font-size-sm);
  }

  .result-actions {
    display: flex;
    gap: var(--space-xs);
    flex-wrap: wrap;
  }

  .results-list {
    list-style: none;
    padding: 0;
    margin: var(--space-md) 0 0;
    display: grid;
    gap: var(--space-xs);
    text-align: left;
  }

  .result-link {
    display: block;
    padding: var(--space-sm);
    background: var(--color-base-200);
    border: 1px solid var(--color-base-300);
    border-radius: var(--radius-0);
    color: inherit;
    text-decoration: none;
    transition: border-color var(--transition-base);
  }

  .result-link:hover { border-color: var(--color-primary); }

  .result-objeto {
    font-size: var(--font-size-sm);
    color: var(--color-base-content);
    margin-bottom: 4px;
  }

  .result-meta {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-sm);
    font-size: var(--font-size-xs);
    font-family: var(--font-mono);
    color: var(--color-secondary);
  }

  .result-valor { color: var(--color-primary); font-weight: 700; }

  .skeleton-row { height: 3.5rem; border-radius: var(--radius-0); }

  .hints {
    margin-top: var(--space-sm);
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-xs) var(--space-sm);
    align-items: baseline;
    font-size: var(--font-size-xs);
    color: var(--color-secondary);
  }

  .hints-label {
    font-weight: 500;
    flex: 0 0 auto;
  }

  .hint-chips {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-xs);
    min-width: 0;
  }

  .hint-chip {
    background: var(--color-base-200);
    border: 1px solid var(--color-base-300);
    padding: 3px 10px;
    border-radius: var(--radius-pill);
    cursor: pointer;
    font-size: var(--font-size-xs);
    color: var(--color-secondary);
    font-family: var(--font-sans);
    transition: all var(--transition-base);
  }

  .hint-chip:hover {
    border-color: var(--color-primary);
    color: var(--color-primary);
  }
</style>
