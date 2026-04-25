<script lang="ts">
  import { resolve } from '../lib/baseUrl';
  // SSR-only: Astro renders this to plain HTML and ships zero JS. The browser
  // handles form submission natively (GET → /baliza/busca?q=…). The PNCP-id
  // shortcut lives on the /busca page so this stays a no-JS entry point.
  const action = resolve('busca');
</script>

<form class="home-busca" method="get" action={action}>
  <label class="sr-only" for="home-busca-input">Buscar no PNCP</label>
  <input
    id="home-busca-input"
    type="search"
    name="q"
    placeholder="Buscar por objeto (ex.: hospital municipal) ou número de controle PNCP…"
    aria-label="Buscar no PNCP"
  />
  <button type="submit" class="btn btn-primary">Buscar</button>
</form>

<style>
  .home-busca {
    display: flex;
    gap: var(--space-2, 0.5rem);
    max-width: 640px;
    margin: 0 auto;
  }
  .home-busca input {
    flex: 1;
    padding: 0.625rem 0.875rem;
    border: 1px solid var(--neutral-200, #e5e7eb);
    border-radius: 4px;
    font-size: var(--text-md, 1rem);
    background: var(--neutral-0, #fff);
  }
  .home-busca input:focus-visible {
    outline: 2px solid var(--bulcao-accent, #3b82f6);
    outline-offset: 2px;
  }
  .sr-only {
    position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px;
    overflow: hidden; clip: rect(0, 0, 0, 0); white-space: nowrap; border: 0;
  }
</style>
