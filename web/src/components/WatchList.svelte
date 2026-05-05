<script lang="ts">
  import { onMount } from 'svelte';
  import {
    hydrateWatches,
    removeWatch,
    watchState,
    type WatchEntry,
  } from '../lib/watchStore.svelte';
  import { resolve } from '../lib/baseUrl';

  // Mirror state lazily so SSR renders an empty list instead of leaking
  // whichever entries exist on the build machine (always empty today, but
  // keeps the invariant "no localStorage reads on the server").
  let mounted = $state(false);
  onMount(() => {
    hydrateWatches();
    mounted = true;
  });

  const entries = $derived(mounted ? watchState.entries : []);

  function labelForType(type: WatchEntry['type']): string {
    if (type === 'agency') return 'Órgão';
    if (type === 'supplier') return 'Fornecedor';
    return 'Consulta';
  }

  function linkFor(entry: WatchEntry): string {
    return resolve(`vigilancia?id=${entry.id}`);
  }
</script>

{#if entries.length > 0}
  <section class="watch-list" aria-labelledby="watch-title">
    <div class="container">
      <header class="watch-head">
        <span class="kicker">Você está acompanhando</span>
        <h2 id="watch-title">
          {entries.length === 1 ? '1 item salvo' : `${entries.length} itens salvos`}
        </h2>
        <p class="watch-sub">
          Salvos neste navegador. Baliza não envia alertas por e-mail — abra esta página
          quando quiser revisitar os órgãos e fornecedores que você marcou.
        </p>
      </header>

      <ul class="watch-grid">
        {#each entries as entry (entry.id)}
          <li class="watch-card">
            <div class="watch-meta">
              <span class="watch-type">{labelForType(entry.type)}</span>
              <span class="watch-cnpj">{entry.filter}</span>
            </div>
            <p class="watch-label">{entry.label}</p>
            <div class="watch-actions">
              <a class="watch-open" href={linkFor(entry)}>Abrir painel →</a>
              <button
                type="button"
                class="watch-remove"
                onclick={() => removeWatch(entry.id)}
                aria-label={`Remover ${entry.label} da lista`}
              >
                Remover
              </button>
            </div>
          </li>
        {/each}
      </ul>
    </div>
  </section>
{/if}

<style>
  .watch-list {
    padding: var(--space-7) 0;
    background: var(--neutral-0);
    border-top: 1px solid var(--neutral-100);
    border-bottom: 1px solid var(--neutral-100);
  }
  .watch-head {
    max-width: 640px;
    display: grid;
    gap: var(--space-2);
    margin-bottom: var(--space-5);
  }
  .watch-head h2 {
    font-family: var(--font-display);
    font-weight: 300;
    font-size: clamp(24px, 2.4vw, 32px);
    letter-spacing: -0.02em;
  }
  .watch-sub {
    color: var(--neutral-500);
    font-size: var(--text-sm);
    line-height: 1.6;
  }
  .watch-grid {
    list-style: none;
    margin: 0;
    padding: 0;
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
    gap: var(--space-3);
  }
  .watch-card {
    display: grid;
    gap: var(--space-2);
    padding: var(--space-4);
    background: var(--neutral-0);
    border: 1px solid var(--neutral-100);
    border-left: 3px solid var(--bulcao-support);
  }
  .watch-meta {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    gap: var(--space-3);
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    letter-spacing: 0.06em;
    color: var(--neutral-500);
  }
  .watch-type {
    text-transform: uppercase;
    color: var(--bulcao-support);
  }
  .watch-cnpj {
    color: var(--neutral-500);
  }
  .watch-label {
    margin: 0;
    font-family: var(--font-display);
    font-size: var(--text-md);
    line-height: 1.3;
    color: var(--bulcao-fg);
    overflow-wrap: anywhere;
  }
  .watch-actions {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: var(--space-3);
    padding-top: var(--space-2);
    border-top: 1px solid var(--neutral-100);
  }
  .watch-open {
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--bulcao-accent);
    border-bottom: 2px solid transparent;
    padding: 2px 0;
  }
  .watch-open:hover,
  .watch-open:focus-visible {
    border-bottom-color: var(--bulcao-accent);
  }
  .watch-remove {
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: var(--neutral-500);
    padding: 2px 4px;
    border: 1px solid transparent;
  }
  .watch-remove:hover {
    color: var(--bulcao-accent);
    border-color: var(--bulcao-accent);
  }
</style>
