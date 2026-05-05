<script lang="ts">
  import {
    cityState,
    hydrateCityContext,
    setCity,
    DEFAULT_CITY,
  } from '../lib/cityContext.svelte';
  import {
    resolveCityFromBrowserLocation,
    searchMunicipalities,
    type SearchMatch,
  } from '../lib/geo';

  interface Props {
    compact?: boolean;
    autofocus?: boolean;
    onselect?: (city: { ibge: string; nome: string; uf: string }) => void;
  }
  let { compact = false, autofocus = false, onselect }: Props = $props();

  let query = $state('');
  let results = $state<SearchMatch[]>([]);
  let loading = $state(false);
  let geoStatus = $state<'idle' | 'locating' | 'denied' | 'error'>('idle');
  let inputEl = $state<HTMLInputElement | null>(null);
  let activeIndex = $state(-1);

  hydrateCityContext();

  // Token to invalidate stale searches: the user can type fast enough to
  // race two `searchMunicipalities` resolutions; only the latest wins.
  let searchToken = 0;
  let debounce: ReturnType<typeof setTimeout> | null = null;

  async function runSearch(term: string) {
    const trimmed = term.trim();
    if (trimmed.length < 2) {
      results = [];
      loading = false;
      return;
    }
    const myToken = ++searchToken;
    loading = true;
    try {
      const matches = await searchMunicipalities(trimmed, 12);
      if (myToken !== searchToken) return; // stale
      results = matches;
    } catch {
      if (myToken !== searchToken) return;
      results = [];
    } finally {
      if (myToken === searchToken) loading = false;
    }
  }

  function onInput(e: Event) {
    const value = (e.target as HTMLInputElement).value;
    query = value;
    activeIndex = -1;
    if (debounce) clearTimeout(debounce);
    debounce = setTimeout(() => runSearch(value), 120);
  }

  function pick(r: SearchMatch) {
    setCity(r, 'storage');
    results = [];
    query = '';
    activeIndex = -1;
    onselect?.(r);
  }

  function onKeydown(e: KeyboardEvent) {
    if (!results.length) return;
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      activeIndex = (activeIndex + 1) % results.length;
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      activeIndex = activeIndex <= 0 ? results.length - 1 : activeIndex - 1;
    } else if (e.key === 'Enter') {
      if (activeIndex >= 0 && activeIndex < results.length) {
        e.preventDefault();
        pick(results[activeIndex]);
      }
    } else if (e.key === 'Escape') {
      results = [];
      activeIndex = -1;
    }
  }

  async function useMyLocation() {
    geoStatus = 'locating';
    try {
      const next = await resolveCityFromBrowserLocation();
      setCity(next, 'storage');
      geoStatus = 'idle';
      onselect?.(next);
    } catch (err) {
      const code = (err as { code?: number }).code;
      geoStatus = code === 1 ? 'denied' : 'error';
    }
  }

  function resetToDefault() {
    // setCity(_, 'default') clears the storage entry internally so a reload
    // resolves back to source: 'default' instead of lingering on 'storage'.
    setCity(DEFAULT_CITY, 'default');
    onselect?.({ ...DEFAULT_CITY });
  }

  $effect(() => {
    if (autofocus && inputEl) inputEl.focus();
  });

  // Clear the pending debounce on unmount so a late timeout can't fire a
  // search into a torn-down component. The searchToken counter handles
  // already-in-flight resolutions.
  $effect(() => () => {
    if (debounce) clearTimeout(debounce);
  });
</script>

<div class="city-picker" class:compact role="search" aria-label="Trocar cidade">
  <div class="active-line">
    <span class="kicker">
      {cityState.source === 'default' ? 'Cidade inicial' : 'Cidade escolhida'}
    </span>
    <strong>{cityState.nome}{cityState.uf ? ` / ${cityState.uf}` : ''}</strong>
    {#if cityState.source !== 'default'}
      <button type="button" class="link" onclick={resetToDefault}>
        usar {DEFAULT_CITY.nome}
      </button>
    {/if}
  </div>

  <label class="field">
    <span class="sr-only">Buscar município</span>
    <input
      class="input-modernist"
      type="search"
      role="combobox"
      placeholder="Digite o nome da cidade (ex.: Porto Velho, Manaus, Recife)"
      aria-label="Buscar município"
      aria-autocomplete="list"
      aria-controls="city-picker-listbox"
      aria-expanded={results.length > 0}
      aria-activedescendant={activeIndex >= 0 ? `city-opt-${activeIndex}` : undefined}
      bind:this={inputEl}
      value={query}
      oninput={onInput}
      onkeydown={onKeydown}
      autocomplete="off"
    />
    <button
      type="button"
      class="geo-btn"
      onclick={useMyLocation}
      disabled={geoStatus === 'locating'}
      aria-label="Usar minha localização atual"
    >
      {geoStatus === 'locating' ? 'Buscando…' : '📍 minha localização'}
    </button>
  </label>

  {#if loading}
    <p class="status" aria-live="polite">Buscando municípios…</p>
  {:else if query.trim().length === 1}
    <p class="status" aria-live="polite">Digite ao menos 2 letras para buscar.</p>
  {/if}

  {#if geoStatus === 'denied'}
    <p class="status warn">Acesso à localização negado. Digite o nome da cidade.</p>
  {:else if geoStatus === 'error'}
    <p class="status warn">Não foi possível obter sua localização.</p>
  {/if}

  {#if results.length > 0}
    <ul id="city-picker-listbox" class="listbox" role="listbox">
      {#each results as r, i (r.ibge)}
        <li
          id={`city-opt-${i}`}
          role="option"
          aria-selected={i === activeIndex}
          class:active={i === activeIndex}
        >
          <button type="button" onclick={() => pick(r)} onmouseenter={() => (activeIndex = i)}>
            <span class="opt-nome">{r.nome}</span>
            <span class="opt-uf">{r.uf || '—'}</span>
            <span class="opt-ibge">IBGE {r.ibge}</span>
          </button>
        </li>
      {/each}
    </ul>
  {/if}
</div>

<style>
  .city-picker {
    display: grid;
    gap: var(--space-3);
    position: relative;
  }
  .compact {
    gap: var(--space-2);
  }
  .active-line {
    display: flex;
    gap: var(--space-2);
    align-items: baseline;
    flex-wrap: wrap;
    font-size: var(--text-sm);
    color: var(--neutral-500);
  }
  .active-line strong {
    font-family: var(--font-display);
    font-weight: 500;
    font-size: var(--text-md);
    color: var(--bulcao-fg);
  }
  .field {
    display: flex;
    gap: var(--space-3);
    align-items: flex-end;
    flex-wrap: wrap;
  }
  .field .input-modernist {
    flex: 1;
    min-width: 220px;
  }
  .geo-btn {
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    letter-spacing: 0.04em;
    text-transform: uppercase;
    color: var(--bulcao-support);
    border-bottom: 2px solid transparent;
    padding: 6px 2px;
  }
  .geo-btn:hover:not(:disabled) {
    border-bottom-color: var(--bulcao-support);
  }
  .geo-btn:disabled {
    opacity: 0.6;
    cursor: progress;
  }
  .link {
    color: var(--bulcao-support);
    text-decoration: underline;
    text-underline-offset: 2px;
    font-size: var(--text-xs);
  }
  .link:hover {
    color: var(--bulcao-accent);
  }
  .status {
    margin: 0;
    font-size: var(--text-xs);
    color: var(--neutral-500);
    font-family: var(--font-mono);
    letter-spacing: 0.04em;
  }
  .status.warn {
    color: var(--bulcao-accent);
  }
  .listbox {
    list-style: none;
    margin: 0;
    padding: 0;
    border: 1px solid var(--neutral-200);
    background: var(--neutral-0);
    max-height: 260px;
    overflow-y: auto;
    box-shadow: var(--shadow-sm);
  }
  .listbox li button {
    width: 100%;
    display: grid;
    grid-template-columns: 1fr auto auto;
    gap: var(--space-3);
    align-items: baseline;
    padding: 10px var(--space-3);
    text-align: left;
    font-size: var(--text-sm);
    border-bottom: 1px solid var(--neutral-100);
    color: var(--bulcao-fg);
  }
  .listbox li:last-child button {
    border-bottom: none;
  }
  .listbox li.active button,
  .listbox li button:focus-visible {
    background: var(--neutral-100);
    color: var(--bulcao-accent);
    outline: none;
  }
  .opt-nome {
    font-family: var(--font-display);
    font-weight: 400;
  }
  .opt-uf {
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    color: var(--bulcao-support);
    letter-spacing: 0.06em;
  }
  .opt-ibge {
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    color: var(--neutral-500);
  }
</style>
