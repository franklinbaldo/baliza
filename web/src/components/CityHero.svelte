<script lang="ts">
  import CityPicker from './CityPicker.svelte';
  import { cityState, hydrateCityContext } from '../lib/cityContext.svelte';

  const BASE = '/baliza/';

  hydrateCityContext();

  let pickerOpen = $state(false);
  const dashboardHref = $derived(`${BASE}municipio?ibge=${cityState.ibge}`);
  const sourceLabel = $derived(
    cityState.source === 'default'
      ? 'Cidade inicial · trocar para a sua'
      : cityState.source === 'storage'
        ? 'Sua cidade salva'
        : 'Cidade no URL',
  );
</script>

<section class="city-hero" aria-labelledby="city-hero-title">
  <div class="container hero-grid">
    <div class="hero-copy">
      <span class="kicker">Painel civic · PNCP</span>
      <h1 id="city-hero-title" class="hero-city">
        O que
        <em class="city-token">
          {cityState.nome}{cityState.uf ? ` / ${cityState.uf}` : ''}
        </em>
        está comprando?
      </h1>
      <p class="hero-lead">
        Acompanhe contratações, órgãos e fornecedores da sua cidade.
        Os dados vêm do PNCP e são arquivados diariamente no Internet Archive,
        com link estável para você citar, investigar ou compartilhar.
      </p>

      <div class="hero-actions">
        <a class="btn btn-primary btn-lg" href={dashboardHref}>
          Ver painel de {cityState.nome}
        </a>
        <button
          type="button"
          class="btn btn-secondary btn-lg"
          aria-expanded={pickerOpen}
          aria-controls="city-hero-picker"
          onclick={() => (pickerOpen = !pickerOpen)}
        >
          {pickerOpen ? 'Fechar seletor' : 'Trocar cidade'}
        </button>
      </div>

      <p class="hero-source" aria-live="polite">{sourceLabel}</p>

      {#if pickerOpen}
        <div id="city-hero-picker" class="picker-wrap">
          <CityPicker autofocus onselect={() => (pickerOpen = false)} />
        </div>
      {/if}
    </div>

    <aside class="hero-ornament" aria-hidden="true">
      <svg class="cobogo" viewBox="0 0 400 400" preserveAspectRatio="xMidYMid slice">
        <rect width="400" height="400" fill="var(--bulcao-support)" />
        <g mask="url(#cobogo-mask)">
          <rect width="400" height="400" fill="var(--bulcao-bg)" />
        </g>
      </svg>
      <svg class="volpi" viewBox="0 0 400 60" preserveAspectRatio="none">
        <rect x="0"   y="0" width="80"  height="60" fill="var(--bulcao-fg)" />
        <rect x="80"  y="0" width="200" height="60" fill="var(--bulcao-bg)" />
        <rect x="280" y="0" width="120" height="60" fill="var(--bulcao-accent)" />
      </svg>
    </aside>
  </div>
</section>

<style>
  .city-hero {
    padding: var(--space-8) 0 var(--space-7);
    border-bottom: 1px solid var(--neutral-100);
    background: var(--neutral-0);
    position: relative;
  }
  .hero-grid {
    display: grid;
    grid-template-columns: 1fr;
    gap: var(--space-6);
    align-items: center;
  }
  .hero-copy {
    max-width: 680px;
    display: grid;
    gap: var(--space-4);
  }
  .hero-city {
    font-family: var(--font-display);
    font-weight: 300;
    font-size: clamp(40px, 6vw, 88px);
    line-height: 1;
    letter-spacing: -0.03em;
    font-variation-settings: 'SOFT' 30, 'opsz' 144;
    color: var(--bulcao-fg);
    text-wrap: balance;
    max-width: 14ch;
  }
  .hero-city .city-token {
    font-style: normal;
    color: var(--bulcao-accent);
    border-bottom: 4px solid var(--bulcao-accent);
    padding-bottom: 2px;
    display: inline-block;
    transition: color var(--duration) var(--ease);
  }
  .hero-lead {
    font-size: var(--text-md);
    color: var(--neutral-500);
    max-width: 58ch;
    line-height: 1.6;
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
    color: var(--neutral-500);
    margin: 0;
  }
  .picker-wrap {
    border-top: 1px solid var(--neutral-100);
    padding-top: var(--space-4);
    margin-top: var(--space-2);
  }
  .hero-ornament {
    display: none;
    position: relative;
    min-height: 320px;
  }
  .hero-ornament .cobogo {
    width: 100%;
    height: 100%;
    aspect-ratio: 1 / 1;
  }
  .hero-ornament .volpi {
    position: absolute;
    left: 0;
    right: 0;
    bottom: -18px;
    height: 36px;
    width: 100%;
  }
  @media (min-width: 960px) {
    .hero-grid {
      grid-template-columns: minmax(0, 1.4fr) minmax(0, 1fr);
      gap: var(--space-7);
    }
    .hero-ornament {
      display: block;
    }
  }
</style>
