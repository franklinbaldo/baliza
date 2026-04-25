<script lang="ts">
  import { cityState, hydrateCityContext } from '../lib/cityContext.svelte';
  import { resolve } from '../lib/baseUrl';

  let { isActive = false } = $props();

  hydrateCityContext();

  const href = $derived(resolve(`municipio?ibge=${cityState.ibge}`));
  const label = $derived(
    cityState.nome
      ? `Município · ${cityState.nome}${cityState.uf ? '/' + cityState.uf : ''}`
      : 'Município',
  );
</script>

<a {href} class="city-nav-link" aria-label={`Painel do município de ${cityState.nome}`} aria-current={isActive ? 'page' : undefined}>
  {label}
</a>

<style>
  .city-nav-link {
    text-decoration: none;
    color: var(--color-text-dim);
    font-family: var(--font-body);
    font-weight: 500;
    font-size: var(--text-sm);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    border: 1px solid transparent;
    border-radius: var(--radius-pill);
    padding: 0.45rem 0.7rem;
    display: inline-flex;
    align-items: center;
    transition: all var(--duration-fast) var(--ease);
  }
  .city-nav-link:hover {
    color: var(--color-text);
    background: color-mix(in srgb, var(--color-azul) 12%, transparent);
    border-color: color-mix(in srgb, var(--color-azul) 28%, transparent);
  }
  .city-nav-link:focus-visible {
    color: var(--color-text);
    border-color: var(--color-azul);
  }
</style>
