<script lang="ts">
  import { fade } from 'svelte/transition';
  import { PROJECT_MISSION, SEARCH_HINTS } from '../lib/homepage-content';
  import type { SearchResult } from '../lib/types';
  
  let query = $state("");
  let results = $state<SearchResult[]>([]);
  let loading = $state(false);

  // Pattern detection for "Direct Jump"
  const isCnpj = $derived(/^\d{14}$/.test(query.replace(/\D/g, "")));
  const isIbge = $derived(/^\d{7}$/.test(query.trim()));
  const isPncpId = $derived(/\d{14}[-/]\d{1}[-/]\d{6}[-/]\d{4}/.test(query.trim()));

  async function handleSearch() {
    if (query.length < 3) {
      results = [];
      return;
    }
    loading = true;
    try {
      await new Promise(r => setTimeout(r, 400));
      // Results logic would go here
    } finally {
      loading = false;
    }
  }
</script>

<section class="hero-section">
  <div class="container hero-inner">
    <h1 class="hero-title">
      {PROJECT_MISSION.title}
    </h1>
    <p class="hero-tagline">{PROJECT_MISSION.tagline}</p>

    <div class="search-box" class:detected={isCnpj || isIbge || isPncpId}>
      <div class="input-wrapper">
        <span class="terminal-prompt">$</span>
        <div class="input-group">
          <input
            type="text"
            bind:value={query}
            placeholder="Busque por Órgão, CNPJ ou Objeto de compra..."
            oninput={handleSearch}
            class="terminal-input"
          />
          {#if loading}
            <div class="valid-indicator spinning">⏳</div>
          {:else if isCnpj || isIbge || isPncpId}
            <div class="valid-indicator" in:fade>✅</div>
          {/if}
          <button class="search-btn">
            Buscar
          </button>
        </div>
      </div>

      {#if isCnpj || isIbge || isPncpId}
        <div class="jump-suggestion">
          <span>🎯 Identificamos um padrão:</span>
          {#if isCnpj}
            <a href={`/baliza/orgao?id=${query}`} class="jump-link">Explorar Órgão (CNPJ: {query})</a>
          {:else if isIbge}
            <a href={`/baliza/municipio?id=${query}`} class="jump-link">Explorar Município (IBGE: {query})</a>
          {:else if isPncpId}
            <a href={`/baliza/contratacao?id=${query}`} class="jump-link">Ver Contratação #{query}</a>
          {/if}
        </div>
      {/if}

      {#if results.length > 0}
         <div class="search-results-preview">
            <!-- Results list placeholder -->
         </div>
      {/if}
      
      <div class="hints">
        <span>Sugestões:</span>
        {#each SEARCH_HINTS as hint (hint)}
          <button class="hint-chip" onclick={() => query = hint}>
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
    background: radial-gradient(circle at top right, var(--color-base-200), transparent);
  }
  .hero-inner {
    max-width: 800px;
    margin: 0 auto;
    text-align: center;
  }
  .hero-title {
    font-size: var(--font-size-3xl);
    margin-bottom: var(--space-xs);
    color: var(--color-base-content);
    line-height: 1.1;
    font-weight: 800;
  }
  .hero-tagline {
    color: var(--color-secondary);
    font-size: var(--font-size-lg);
    margin-bottom: var(--space-lg);
    font-weight: 400;
  }
  .search-box {
    background: var(--color-base-100);
    padding: var(--space-md);
    border: 1px solid var(--color-base-300);
    border-radius: var(--radius-box);
    box-shadow: 0 10px 30px -10px rgba(0,0,0,0.3);
    text-align: left;
  }
  .input-wrapper {
    display: flex;
    align-items: center;
    background: var(--color-base-200);
    border: 1px solid var(--color-base-300);
    border-radius: var(--radius-sm);
    padding-left: var(--space-sm);
    transition: border-color var(--transition-base), box-shadow var(--transition-base);
  }
  .input-wrapper:focus-within {
    border-color: var(--color-primary);
    box-shadow: 0 0 0 2px color-mix(in srgb, var(--color-primary) 20%, transparent);
  }
  .terminal-prompt {
    font-family: var(--font-mono);
    color: var(--color-primary);
    font-weight: 700;
    margin-right: var(--space-xs);
    user-select: none;
  }
  .input-group {
    display: flex;
    gap: var(--space-xs);
    position: relative;
    flex: 1;
  }
  .terminal-input {
    flex: 1;
    padding: var(--space-sm) 0;
    border: none;
    background: transparent;
    color: var(--color-base-content);
    font-family: var(--font-mono);
    font-size: var(--font-size-base);
    outline: none;
  }
  .terminal-input::placeholder {
    font-family: var(--font-sans);
    color: var(--color-secondary);
    opacity: 0.7;
  }
  .search-btn {
    background: var(--color-primary);
    color: var(--color-base-100);
    padding: 0 var(--space-md);
    border: none;
    border-radius: 0 var(--radius-sm) var(--radius-sm) 0;
    font-weight: 600;
    cursor: pointer;
    transition: opacity var(--transition-base);
  }
  .search-btn:hover {
    opacity: 0.9;
  }
  .search-box.detected .input-group {
    border-color: var(--color-primary);
    box-shadow: 0 0 0 4px rgba(59, 130, 246, 0.1);
  }

  .valid-indicator {
    position: absolute;
    right: 120px;
    top: 50%;
    transform: translateY(-50%);
    font-size: 1.2rem;
    pointer-events: none;
    z-index: 10;
  }
  .valid-indicator.spinning {
    animation: spin 1s linear infinite;
  }
  @keyframes spin { from { transform: translateY(-50%) rotate(0deg); } to { transform: translateY(-50%) rotate(360deg); } }

  .hints {
    margin-top: var(--space-sm);
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-xs);
    align-items: center;
    font-size: var(--font-size-xs);
    color: var(--color-secondary);
  }
  .hint-chip {
    background: var(--color-base-200);
    border: 1px solid var(--color-base-300);
    padding: 2px 8px;
    border-radius: 99px;
    cursor: pointer;
    font-size: var(--font-size-xs);
    color: var(--color-secondary);
  }
  .hint-chip:hover {
    border-color: var(--color-primary);
    color: var(--color-primary);
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
    animation: fadeIn 0.3s ease;
  }
  .jump-suggestion span { font-size: var(--font-size-xs); color: var(--color-secondary); font-weight: 600; }
  .jump-link { 
    font-weight: 800; color: var(--color-primary); text-decoration: none; 
    font-family: var(--font-mono); font-size: var(--font-size-sm);
  }
  .jump-link:hover { text-decoration: underline; }

  @keyframes fadeIn { from { opacity: 0; transform: translateY(-5px); } to { opacity: 1; transform: translateY(0); } }
</style>
