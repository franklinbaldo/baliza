<script lang="ts">
  import { PROJECT_MISSION, SEARCH_HINTS } from '../lib/homepage-content';
  import EmptyState from './EmptyState.svelte';

  let query = $state('');
  let loading = $state(false);

  const cleanQuery = $derived(query.trim());
  const isCnpj    = $derived(/^\d{14}$/.test(cleanQuery.replace(/\D/g, '')));
  const isIbge    = $derived(/^\d{7}$/.test(cleanQuery));
  const isPncpId  = $derived(/\d{14}[-/]\d{1}[-/]\d{6}[-/]\d{4}/.test(cleanQuery));
  const hasPattern = $derived(isCnpj || isIbge || isPncpId);
  const showEmpty  = $derived(cleanQuery.length >= 3 && !loading && !hasPattern);

  function handleSearch() {
    if (cleanQuery.length < 3) return;
    loading = true;
    setTimeout(() => { loading = false; }, 200);
  }
</script>

<section class="hero-section">
  <div class="container hero-inner">
    <h1 class="hero-title">{PROJECT_MISSION.title}</h1>
    <p class="hero-tagline">{PROJECT_MISSION.tagline}</p>

    <div class="search-box" class:detected={hasPattern}>
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
            oninput={handleSearch}
            autocomplete="off"
            spellcheck={false}
            aria-label="Buscar contratos públicos"
          />
          {#if query}
            <button type="button" class="clear-btn" onclick={() => (query = '')} aria-label="Limpar busca">×</button>
          {/if}
        </label>

        {#if loading}
          <div class="valid-indicator cg-pulse" aria-hidden="true">⏳</div>
        {:else if hasPattern}
          <div class="valid-indicator" aria-hidden="true">✅</div>
        {/if}

        <button class="search-btn btn btn-primary" disabled={loading}>Buscar</button>
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
          {:else if isPncpId}
            <a href={`/baliza/contratacao?id=${cleanQuery}`} class="jump-link">
              Ver Contratação <span class="badge badge-sm badge-info">#{cleanQuery}</span>
            </a>
          {/if}
        </div>
      {:else if showEmpty}
        <div>
          <EmptyState
            title="Nenhum padrão detectado"
            message="Use um CNPJ (14 dígitos), código IBGE (7 dígitos) ou identificador PNCP para ir direto ao resultado."
          />
        </div>
      {/if}

      <div class="hints" role="status" aria-live="polite">
        <span class="hints-label">Sugestões:</span>
        {#each SEARCH_HINTS as hint (hint)}
          <button class="hint-chip" onclick={() => (query = hint)}>
            {hint}
          </button>
        {/each}
      </div>
    </div>
  </div>
</section>

<style>
  .hero-section {
    padding: var(--space-3xl) 0 var(--space-xl);
    background:
      radial-gradient(circle at top right, var(--color-base-200), transparent),
      linear-gradient(to right, rgba(255,255,255,0.02) 1px, transparent 1px),
      linear-gradient(to bottom, rgba(255,255,255,0.02) 1px, transparent 1px);
    background-size: 100% 100%, 20px 20px, 20px 20px;
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
    font-size: var(--font-size-lg);
    margin-bottom: var(--space-lg);
  }

  .search-box {
    background: var(--color-base-100);
    padding: var(--space-md);
    border: 1px solid var(--color-base-300);
    border-radius: var(--radius-box);
    box-shadow: 0 10px 30px -10px rgba(0, 0, 0, 0.3);
  }

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
    box-shadow: var(--shadow-glow);
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
