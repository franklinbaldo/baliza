<script lang="ts">
  import CityPicker from './CityPicker.svelte';
  import { cityState, hydrateCityContext, setCity } from '../lib/cityContext.svelte';
  import { resolve } from '../lib/baseUrl';
  import { getUserCoordinates, getCityFromCoords, getIBGECode, ufNomeToSigla } from '../lib/geo';

  hydrateCityContext();

  let pickerOpen = $state(false);
  const dashboardHref = $derived(resolve(`municipio?ibge=${cityState.ibge}`));
  const sourceLabel = $derived(
    cityState.source === 'default'
      ? 'Cidade inicial · trocar para a sua'
      : cityState.source === 'storage'
        ? 'Sua cidade salva'
        : 'Cidade no URL',
  );

  type GeoStatus = 'idle' | 'locating' | 'ready' | 'denied' | 'error';
  let geoStatus = $state<GeoStatus>('idle');

  async function handleFindLocal() {
    geoStatus = 'locating';
    try {
      const coords = await getUserCoordinates();
      const cityData = await getCityFromCoords(coords.latitude, coords.longitude);
      const code = await getIBGECode(cityData.city, cityData.state);
      if (!code) throw new Error('Não foi possível localizar o código IBGE para este município.');
      setCity({ ibge: code, nome: cityData.city, uf: ufNomeToSigla(cityData.state) }, 'storage');
      geoStatus = 'ready';
    } catch (err) {
      console.error(err);
      const code = (err as { code?: number }).code;
      geoStatus = code === 1 ? 'denied' : 'error';
    }
  }
</script>

<section class="city-hero" aria-labelledby="city-hero-title">
  <div class="container hero-grid">
    <div class="hero-copy">
      <span class="kicker">Painel cívico · O que é o Baliza?</span>
      <h1 id="city-hero-title" class="hero-city">
        O que
        <em class="city-token">
          {cityState.nome}{cityState.uf ? ` / ${cityState.uf}` : ''}
        </em>
        está comprando?
      </h1>
      <p class="hero-lead">
        <strong>B</strong>ackup <strong>A</strong>berto de <strong>Li</strong>citações <strong>Z</strong>elando pelo <strong>A</strong>cesso.
      </p>

      <form class="hero-search" method="get" action={resolve('busca')}>
        <input
          type="search"
          name="q"
          value={cityState.nome}
          placeholder="Ex: hospital municipal, merenda, obras..."
          aria-label="Buscar no PNCP"
        />
        <button type="submit" class="btn btn-primary btn-search">
          <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"></circle><line x1="21" y1="21" x2="16.65" y2="16.65"></line></svg>
          Buscar
        </button>
      </form>

      <div class="hero-actions">
        <a class="btn btn-primary btn-lg" href={dashboardHref}>
          Ver painel de {cityState.nome}
        </a>

        {#if cityState.source === 'default'}
          <button
            type="button"
            class="btn btn-secondary btn-lg geo-btn"
            onclick={handleFindLocal}
            disabled={geoStatus === 'locating'}
          >
            {#if geoStatus === 'locating'}
              <span class="spinner"></span> Localizando...
            {:else}
              📍 Usar minha localização
            {/if}
          </button>
        {/if}

        <button
          type="button"
          class="btn btn-outline btn-lg"
          aria-expanded={pickerOpen}
          aria-controls="city-hero-picker"
          onclick={() => (pickerOpen = !pickerOpen)}
        >
          {pickerOpen ? 'Fechar seletor' : 'Buscar outra cidade'}
        </button>
      </div>

      {#if geoStatus === 'denied'}
        <p class="hero-error" aria-live="polite">Acesso à localização negado. Use o seletor manual.</p>
      {:else if geoStatus === 'error'}
        <p class="hero-error" aria-live="polite">Não conseguimos determinar sua localização. Use o seletor manual.</p>
      {:else}
        <p class="hero-source" aria-live="polite">{sourceLabel}</p>
      {/if}

      {#if pickerOpen}
        <div id="city-hero-picker" class="picker-wrap">
          <CityPicker autofocus onselect={() => (pickerOpen = false)} />
        </div>
      {/if}
    </div>
  </div>
</section>

<style>
  .city-hero {
    padding: var(--space-6) 0 var(--space-4);
    background:
      linear-gradient(135deg, color-mix(in srgb, var(--color-bg) 88%, transparent) 0 58%, color-mix(in srgb, var(--color-surface-sunk) 72%, transparent) 58% 100%),
      var(--color-bg);
    position: relative;
    overflow: hidden;
  }
  .city-hero::before {
    content: "";
    position: absolute;
    inset: auto 0 0;
    height: 8px;
    background: repeating-linear-gradient(90deg, var(--color-accent) 0 28px, var(--color-ouro) 28px 40px, var(--color-azul) 40px 68px, var(--color-tijolo) 68px 80px);
  }
  .hero-grid {
    display: flex;
    flex-direction: column;
    align-items: center;
    text-align: center;
    gap: var(--space-4);
  }
  .hero-copy {
    max-width: 800px;
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: var(--space-4);
  }
  .hero-city {
    font-family: var(--font-display);
    font-weight: 300;
    font-size: clamp(32px, 5vw, 64px);
    line-height: 1.1;
    letter-spacing: 0;
    font-variation-settings: 'SOFT' 30, 'opsz' 144;
    color: var(--color-text);
    text-wrap: balance;
    max-width: 14ch;
  }
  .hero-city .city-token {
    font-style: normal;
    color: var(--color-accent);
    border-bottom: 6px solid color-mix(in srgb, var(--color-ouro) 78%, var(--color-accent));
    padding-bottom: 0;
    display: inline-block;
    transition: color var(--duration) var(--ease);
  }
  .hero-lead {
    font-size: var(--text-md);
    color: var(--color-text-dim);
    max-width: 58ch;
    line-height: 1.6;
  }
  .hero-search {
    display: flex;
    width: 100%;
    max-width: 600px;
    gap: var(--space-2);
    margin: var(--space-2) 0;
  }
  .hero-search input {
    flex: 1;
    padding: 12px 16px;
    font-size: var(--text-md);
    border: 2px solid var(--color-border);
    border-radius: var(--radius-sm);
    background: var(--color-surface);
    color: var(--color-text);
    transition: all var(--duration-fast) var(--ease);
  }
  .hero-search input:focus-visible {
    outline: none;
    border-color: var(--color-azul);
    box-shadow: var(--shadow-glow);
    background: var(--color-bg);
  }
  .btn-search {
    gap: var(--space-2);
    padding: 0 var(--space-4);
  }
  .hero-actions {
    display: flex;
    gap: var(--space-3);
    flex-wrap: wrap;
    align-items: center;
  }
  .hero-source {
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: var(--color-text-dim);
    margin: 0;
  }
  .picker-wrap {
    border-top: 1px solid var(--color-border);
    padding-top: var(--space-4);
    margin-top: var(--space-2);
    background: color-mix(in srgb, var(--color-surface) 74%, transparent);
    border-radius: var(--radius-lg) var(--radius-lg) 0 var(--radius-lg);
    padding: var(--space-4);
  }
  .geo-btn { position: relative; }
  .spinner {
    display: inline-block;
    width: 1rem;
    height: 1rem;
    border: 2px solid currentColor;
    border-top-color: transparent;
    border-radius: 50%;
    animation: spin 1s linear infinite;
  }
  @keyframes spin { to { transform: rotate(360deg); } }
  .hero-error {
    font-size: var(--text-sm);
    color: var(--color-error);
    margin: 0;
  }
</style>
