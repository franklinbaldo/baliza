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
    color: var(--neutral-500);
    font-family: var(--font-body);
    font-weight: 500;
    font-size: var(--text-sm);
    white-space: nowrap;
    border-bottom: 2px solid transparent;
    transition: all var(--duration-fast) var(--ease);
  }
  .city-nav-link:hover {
    color: var(--bulcao-accent);
    border-bottom-color: var(--bulcao-accent);
  }
  .city-nav-link:focus-visible {
    color: var(--bulcao-accent);
    border-bottom-color: var(--bulcao-accent);
  }
</style>
