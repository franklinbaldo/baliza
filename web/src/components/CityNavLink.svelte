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

<a {href} aria-label={`Painel do município de ${cityState.nome}`} aria-current={isActive ? 'page' : undefined}>
  {label}
</a>

