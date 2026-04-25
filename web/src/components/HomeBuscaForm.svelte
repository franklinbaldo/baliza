<script lang="ts">
  import { resolve } from '../lib/baseUrl';
  import { isPncpId } from '../lib/pncpId';

  let value = $state('');

  function handleSubmit(ev: Event) {
    ev.preventDefault();
    const term = value.trim();
    if (!term) return;
    // PNCP-ID shortcut stays on the homepage — jump straight to the
    // contratacao permalink, skipping the /busca round-trip.
    if (isPncpId(term)) {
      if (typeof window !== 'undefined') {
        window.location.assign(resolve(`contratacao?id=${term}`));
      }
      return;
    }
    if (typeof window !== 'undefined') {
      window.location.assign(resolve(`busca?q=${encodeURIComponent(term)}`));
    }
  }
</script>

<form class="home-busca" onsubmit={handleSubmit}>
  <label class="sr-only" for="home-busca-input">Buscar no PNCP</label>
  <input
    id="home-busca-input"
    type="search"
    bind:value
    placeholder="Buscar por objeto (ex.: hospital municipal) ou número de controle PNCP…"
    aria-label="Buscar no PNCP"
  />
  <button type="submit" class="btn btn-primary">Buscar</button>
</form>

<style>
  .home-busca {
    display: flex;
    gap: var(--space-2);
    max-width: 640px;
    margin: 0 auto;
  }
  .home-busca input {
    flex: 1;
    padding: 0.625rem 0.875rem;
    border: 1px solid var(--neutral-200, #e5e7eb);
    border-radius: 4px;
    font-size: var(--text-md);
    background: var(--neutral-0, #fff);
  }
  .home-busca input:focus-visible {
    outline: 2px solid var(--bulcao-accent);
    outline-offset: 2px;
  }
  .sr-only {
    position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px;
    overflow: hidden; clip: rect(0, 0, 0, 0); white-space: nowrap; border: 0;
  }
</style>
