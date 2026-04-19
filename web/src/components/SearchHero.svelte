<script lang="ts">
  import { onDestroy } from 'svelte';
  import { PROJECT_MISSION, SEARCH_HINTS } from '../lib/homepage-content';
  import { isPncpId, parsePncpId } from '../lib/pncpId';
  import { formatBRL, formatDate, normalizeSearchInput } from '../lib/format';
  import EmptyState from './EmptyState.svelte';
  import AlertBanner from './AlertBanner.svelte';

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
  let debounceTimer: ReturnType<typeof setTimeout> | null = null;
  let searchToken = 0;

  function truncate(s: string, n: number) {
    if (!s) return '';
    return s.length > n ? s.slice(0, n - 1) + '…' : s;
  }

  async function runSearch(term: string) {
    const token = ++searchToken;
    searching = true;
    searchError = null;
    try {
      const url = `${SEARCH_ENDPOINT}?q=${encodeURIComponent(term)}&tipos_documento=edital&pagina=1`;
      const res = await fetch(url);
      if (!res.ok) throw new Error('Falha ao consultar o PNCP. Tente novamente em instantes.');
      const json = (await res.json()) as { items?: SearchItem[] };
      if (token !== searchToken) return;
      results = (json.items || []).slice(0, 10);
      didSearch = true;
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
      return;
    }
    debounceTimer = setTimeout(() => runSearch(term), DEBOUNCE_MS);
  }

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
</script>

<section class="hero-section">
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
        </label>

        {#if searching}
          <div class="valid-indicator cg-pulse" aria-hidden="true">⏳</div>
        {:else if hasPattern}
          <div class="valid-indicator" aria-hidden="true">✅</div>
        {/if}

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
          <ul class="results-list" role="listbox" aria-label="Resultados da busca PNCP">
            {#each results as item (item.numero_controle_pncp)}
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
        {#each SEARCH_HINTS as hint (hint)}
          <button type="button" class="hint-chip" onclick={() => { query = hint; scheduleSearch(hint); }}>
            {hint}
          </button>
        {/each}
      </div>
    </form>
  </div>
</section>

<style>
  .hero-section {
    padding: var(--space-3xl) 0 var(--space-xl);
    background:
      linear-gradient(to right, rgba(255,255,255,0.02) 1px, transparent 1px),
      linear-gradient(to bottom, rgba(255,255,255,0.02) 1px, transparent 1px);
    background-size: 20px 20px, 20px 20px;
  }

  .hero-inner {
    max-width: 800px;
    text-align: center;
  }

  .hero-title {
    font-size: var(--font-size-3xl);
    margin-bottom: var(--space-xs);
    color: var(--color-base-content);
    line-height: 1.1;
  }

  .hero-tagline {
    color: var(--color-secondary);
    font-size: var(--font-size-md);
    line-height: 1.6;
    margin-bottom: var(--space-lg);
  }

  .search-box {
    background: var(--color-base-100);
    padding: var(--space-md);
    border: 1px solid var(--color-base-300);
    border-radius: var(--radius-box);
    box-shadow: var(--shadow-md);
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
    padding: 0.75rem 1rem;
    border: 1px solid var(--color-base-300);
    border-radius: var(--radius-sm);
    background: var(--color-base-200);
    transition: border-color var(--transition-base), box-shadow var(--transition-base);
    cursor: text;
  }

  .input-label:focus-within {
    border-color: var(--color-primary);
    box-shadow: inset 0 0 0 2px var(--color-primary);
  }

  .search-icon {
    width: 1.1rem;
    height: 1.1rem;
    opacity: 0.55;
    flex-shrink: 0;
    color: var(--color-secondary);
  }

  input {
    flex: 1;
    border: none;
    background: transparent;
    outline: none;
    font-size: var(--font-size-lg);
    color: var(--color-base-content);
    font-family: var(--font-sans);
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
    font-size: 1.2rem;
    pointer-events: none;
  }

  .search-btn {
    flex-shrink: 0;
    font-size: var(--font-size-base);
    border-radius: var(--radius-sm);
  }

  .search-box.detected .input-label {
    border-color: var(--color-primary);
  }

  .jump-suggestion {
    margin-top: var(--space-md);
    padding: var(--space-sm);
    background: var(--color-base-200);
    border: 1px dashed var(--color-primary);
    border-radius: var(--radius-sm);
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
    border-radius: var(--radius-sm);
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

  .skeleton-row { height: 3.5rem; border-radius: var(--radius-sm); }

  .hints {
    margin-top: var(--space-sm);
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-xs);
    align-items: center;
    font-size: var(--font-size-xs);
    color: var(--color-secondary);
  }

  .hints-label { font-weight: 500; }

  .hint-chip {
    background: var(--color-base-200);
    border: 1px solid var(--color-base-300);
    padding: 3px 10px;
    border-radius: var(--radius-full);
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
