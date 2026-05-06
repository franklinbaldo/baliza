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
    autofocus?: boolean;
    onselect?: (city: { ibge: string; nome: string; uf: string }) => void;
  }
  let { autofocus = false, onselect }: Props = $props();

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

<div role="search" aria-label="Trocar cidade">
  <header>
    <small>{cityState.source === 'default' ? 'Cidade inicial' : 'Cidade escolhida'}</small>
    <strong>{cityState.nome}{cityState.uf ? ` / ${cityState.uf}` : ''}</strong>
    {#if cityState.source !== 'default'}
      <button type="button" class="outline" onclick={resetToDefault}>
        usar {DEFAULT_CITY.nome}
      </button>
    {/if}
  </header>

  <div role="group" aria-label="Buscar município">
    <input
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
    <button type="button" class="outline" onclick={useMyLocation} disabled={geoStatus === 'locating'} aria-busy={geoStatus === 'locating'} aria-label="Usar minha localização atual">
      {#if geoStatus === 'locating'}
        Buscando…
      {:else}
        <svg data-icon aria-hidden="true" viewBox="0 0 24 24"><path d="M12 22s7-7.5 7-13a7 7 0 1 0-14 0c0 5.5 7 13 7 13Z"></path><circle cx="12" cy="9" r="2.5"></circle></svg>
        minha localização
      {/if}
    </button>
  </div>

  {#if loading}
    <small aria-live="polite" aria-busy="true">Buscando municípios…</small>
  {:else if query.trim().length === 1}
    <small aria-live="polite">Digite ao menos 2 letras para buscar.</small>
  {/if}

  {#if geoStatus === 'denied'}
    <small role="alert">Acesso à localização negado. Digite o nome da cidade.</small>
  {:else if geoStatus === 'error'}
    <small role="alert">Não foi possível obter sua localização.</small>
  {/if}

  {#if results.length > 0}
    <ul id="city-picker-listbox" role="listbox">
      {#each results as r, i (r.ibge)}
        <li id={`city-opt-${i}`} role="option" aria-selected={i === activeIndex}>
          <button type="button" class="outline" onclick={() => pick(r)} onmouseenter={() => (activeIndex = i)}>
            <strong>{r.nome}</strong>
            <small>{r.uf || '—'}</small>
            <code>IBGE {r.ibge}</code>
          </button>
        </li>
      {/each}
    </ul>
  {/if}
</div>

