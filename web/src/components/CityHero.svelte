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
  <header>
    <hgroup>
      <p class="eyebrow">Painel cívico · O que é o Baliza?</p>
      <h1 id="city-hero-title">
        O que <mark>{cityState.nome}{cityState.uf ? ` / ${cityState.uf}` : ''}</mark> está comprando?
      </h1>
      <p>
        <strong>B</strong>ackup <strong>A</strong>berto de <strong>Li</strong>citações <strong>Z</strong>elando pelo <strong>A</strong>cesso.
      </p>
    </hgroup>
  </header>

  <form role="search" method="get" action={resolve('busca')}>
    {#if cityState.source !== 'default'}
      <input type="hidden" name="ibge" value={cityState.ibge} />
    {/if}
    <div role="group">
      <input
        type="search"
        name="q"
        placeholder={cityState.source === 'default'
          ? 'Ex: hospital municipal, merenda, obras...'
          : `Buscar em ${cityState.nome} — ex.: merenda, obras...`}
        aria-label="Buscar no PNCP"
      />
      <button type="submit">Buscar</button>
    </div>
  </form>

  <div role="group">
    <a href={dashboardHref} role="button">Ver painel de {cityState.nome}</a>

    {#if geoStatus !== 'ready'}
      <button type="button" class="outline contrast" onclick={handleFindLocal} disabled={geoStatus === 'locating'} aria-busy={geoStatus === 'locating'}>
        <svg data-icon aria-hidden="true" viewBox="0 0 24 24"><path d="M12 22s7-7.5 7-13a7 7 0 1 0-14 0c0 5.5 7 13 7 13Z"></path><circle cx="12" cy="9" r="2.5"></circle></svg>
        {geoStatus === 'locating' ? 'Localizando…' : 'Usar minha localização'}
      </button>
    {/if}
  </div>

  <p>
    <a
      href="#city-hero-picker"
      class="contrast"
      aria-expanded={pickerOpen}
      aria-controls="city-hero-picker"
      onclick={(e) => { e.preventDefault(); pickerOpen = !pickerOpen; }}
    >
      {pickerOpen ? 'Fechar seletor' : 'Buscar outra cidade'}
    </a>
  </p>

  {#if geoStatus === 'denied'}
    <small role="alert" aria-live="polite">Acesso à localização negado. Use o seletor manual.</small>
  {:else if geoStatus === 'error'}
    <small role="alert" aria-live="polite">Não conseguimos determinar sua localização. Use o seletor manual.</small>
  {:else}
    <small aria-live="polite">{sourceLabel}</small>
  {/if}

  {#if pickerOpen}
    <div id="city-hero-picker">
      <CityPicker autofocus onselect={() => (pickerOpen = false)} />
    </div>
  {/if}
</section>

