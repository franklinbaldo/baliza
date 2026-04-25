<script lang="ts">
  import CityPicker from './CityPicker.svelte';
  import { cityState, hydrateCityContext } from '../lib/cityContext.svelte';
  import { resolve } from '../lib/baseUrl';

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

    <aside class="hero-ornament azulejo-triangulos" aria-hidden="true">
      <div class="concrete-slab">
        <span>Baliza</span>
      </div>
      <div class="curve-cut"></div>
    </aside>
  </div>
</section>

<style>
  .city-hero {
    padding: var(--space-9) 0 var(--space-7);
    border-bottom: 1px solid var(--color-border);
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
  .hero-ornament {
    display: none;
    position: relative;
    min-height: 320px;
    border-radius: 0 var(--radius-arch) 0 var(--radius-lg);
    border: 1px solid var(--color-border);
    box-shadow: var(--shadow-pool);
    overflow: hidden;
  }
  .concrete-slab {
    position: absolute;
    inset: auto 0 0 18%;
    min-height: 54%;
    background: color-mix(in srgb, var(--color-text) 92%, transparent);
    color: var(--color-bg);
    border-radius: var(--radius-arch) 0 0 0;
    display: flex;
    align-items: flex-end;
    justify-content: flex-start;
    padding: var(--space-5);
  }
  .concrete-slab span {
    font-family: var(--font-logo);
    font-size: clamp(4rem, 8vw, 7rem);
    line-height: 0.82;
  }
  .curve-cut {
    position: absolute;
    width: 9rem;
    height: 9rem;
    right: -2rem;
    top: -2rem;
    border: 1.5rem solid color-mix(in srgb, var(--color-bg) 94%, transparent);
    border-radius: var(--radius-pill);
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
