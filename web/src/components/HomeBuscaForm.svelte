<script lang="ts">
  import { resolve } from '../lib/baseUrl';
  // SSR-only: Astro renders this to plain HTML and ships zero JS. The browser
  // handles form submission natively (GET → /baliza/busca?q=…). The PNCP-id
  // shortcut lives on the /busca page so this stays a no-JS entry point.
  const action = resolve('busca');
</script>

<form role="search" method="get" action={action}>
  <label for="home-busca-input" class="sr-only">Buscar no PNCP</label>
  <fieldset>
    <input
      id="home-busca-input"
      type="search"
      name="q"
      placeholder="Buscar por objeto (ex.: hospital municipal) ou número de controle PNCP…"
      aria-label="Buscar no PNCP"
    />
    <button type="submit">Buscar</button>
  </fieldset>
</form>

