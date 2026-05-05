<script lang="ts">
  import CityPicker from './CityPicker.svelte';
  import { cityState, hydrateCityContext, setCity } from '../lib/cityContext.svelte';
  import { resolve } from '../lib/baseUrl';
  import { resolveCityFromBrowserLocation } from '../lib/geo';

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
      const city = await resolveCityFromBrowserLocation();
      setCity(city, 'storage');
      geoStatus = 'ready';
    } catch (err) {
      console.error(err);
      const code = (err as { code?: number }).code;
      geoStatus = code === 1 ? 'denied' : 'error';
    }
  }

  // Silently re-detect the visitor's city when (a) they're still on the
  // Porto Velho default (no URL or storage pin) and (b) the browser has a
  // 'granted' geolocation permission from a previous session. Never
  // triggers the permission prompt — new visitors keep seeing the default
  // until they click the manual button. Combined with WowStrip below, the
  // returning visitor's city snaps in without any interaction.
  async function maybeAutoDetect(): Promise<void> {
    if (cityState.source !== 'default') return;
    if (typeof navigator === 'undefined' || !('permissions' in navigator)) return;
    try {
      const status = await navigator.permissions.query({ name: 'geolocation' as PermissionName });
      if (status.state !== 'granted') return;
      const city = await resolveCityFromBrowserLocation();
      setCity(city, 'storage');
    } catch {
      // Silent — the manual "Usar minha localização" button stays available
      // for anyone the silent path can't help.
    }
  }
  $effect(() => {
    void maybeAutoDetect();
  });
</script>

<section aria-labelledby="city-hero-title">
  <div>
    <div>
      <span>Painel cívico · O que é o Baliza?</span>
      <h1 id="city-hero-title">
        O que
        <em>
          {cityState.nome}{cityState.uf ? ` / ${cityState.uf}` : ''}
        </em>
        está comprando?
      </h1>
      <p>
        <strong>B</strong>ackup <strong>A</strong>berto de <strong>Li</strong>citações <strong>Z</strong>elando pelo <strong>A</strong>cesso.
      </p>

      <form method="get" action={resolve('busca')}>
        {#if cityState.source !== 'default'}
          <input type="hidden" name="ibge" value={cityState.ibge} />
        {/if}
        <input
          type="search"
          name="q"
          placeholder={cityState.source === 'default'
            ? 'Ex: hospital municipal, merenda, obras...'
            : `Buscar em ${cityState.nome} — ex.: merenda, obras...`}
          aria-label="Buscar no PNCP"
        />
        <button type="submit">
          <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"></circle><line x1="21" y1="21" x2="16.65" y2="16.65"></line></svg>
          Buscar
        </button>
      </form>

      <div>
        <a href={dashboardHref}>
          Ver painel de {cityState.nome}
        </a>

        {#if geoStatus !== 'ready'}
          <button
            type="button"
            onclick={handleFindLocal}
            disabled={geoStatus === 'locating'}
          >
            {#if geoStatus === 'locating'}
              <span></span> Localizando...
            {:else}
              📍 Usar minha localização
            {/if}
          </button>
        {/if}

        <button
          type="button"
          aria-expanded={pickerOpen}
          aria-controls="city-hero-picker"
          onclick={() => (pickerOpen = !pickerOpen)}
        >
          {pickerOpen ? 'Fechar seletor' : 'Buscar outra cidade'}
        </button>
      </div>

      {#if geoStatus === 'denied'}
        <p aria-live="polite">Acesso à localização negado. Use o seletor manual.</p>
      {:else if geoStatus === 'error'}
        <p aria-live="polite">Não conseguimos determinar sua localização. Use o seletor manual.</p>
      {:else}
        <p aria-live="polite">{sourceLabel}</p>
      {/if}

      {#if pickerOpen}
        <div id="city-hero-picker">
          <CityPicker autofocus onselect={() => (pickerOpen = false)} />
        </div>
      {/if}
    </div>
  </div>
</section>

