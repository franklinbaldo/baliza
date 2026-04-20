<script lang="ts">
  import { onMount } from 'svelte';
  import { readWatches, removeWatch, type Watch } from '../lib/watches';

  let watches = $state<Watch[]>([]);
  let mounted = $state(false);

  function refresh() {
    watches = readWatches();
  }

  onMount(() => {
    mounted = true;
    refresh();
    // Cross-tab sync: another tab saving a watch should update this list
    // without requiring a full reload.
    const onStorage = (ev: StorageEvent) => {
      if (ev.key === 'baliza.watches') refresh();
    };
    window.addEventListener('storage', onStorage);
    return () => window.removeEventListener('storage', onStorage);
  });

  function drop(slug: string) {
    removeWatch(slug);
    refresh();
  }

  function hrefFor(w: Watch): string {
    if (w.kind === 'query' && w.term) {
      return `/baliza/?q=${encodeURIComponent(w.term)}`;
    }
    if (w.kind === 'cnpj-agency' && w.cnpj) {
      return `/baliza/orgao?cnpj=${w.cnpj}`;
    }
    if (w.kind === 'cnpj-supplier' && w.cnpj) {
      return `/baliza/fornecedor?cnpj=${w.cnpj}`;
    }
    return '/baliza/';
  }

  function feedFor(w: Watch): string {
    return `/baliza/alertas/${w.slug}.xml`;
  }

  const KIND_LABEL: Record<Watch['kind'], string> = {
    query: 'Busca',
    'cnpj-agency': 'Órgão',
    'cnpj-supplier': 'Fornecedor',
  };
</script>

{#if mounted && watches.length > 0}
  <section class="watchlist" aria-labelledby="watchlist-title" data-testid="my-watches">
    <div class="container">
      <div class="watchlist-head">
        <h2 id="watchlist-title">Minhas vigilâncias</h2>
        <p>
          Consultas salvas localmente neste navegador. Quando o feed RSS estiver disponível, cada vigilância terá novidades publicadas em <code>/alertas/{'{slug}'}.xml</code>.
        </p>
      </div>
      <ul class="watch-items">
        {#each watches as w (w.slug)}
          <li class="watch-item" data-testid={`watch-item-${w.slug}`}>
            <div class="watch-main">
              <a href={hrefFor(w)} class="watch-label">{w.label}</a>
              <span class={`watch-kind kind-${w.kind}`}>{KIND_LABEL[w.kind]}</span>
            </div>
            <div class="watch-side">
              <code class="watch-feed">{feedFor(w)}</code>
              <button
                type="button"
                class="watch-remove"
                aria-label={`Remover vigilância ${w.label}`}
                onclick={() => drop(w.slug)}
              >
                ×
              </button>
            </div>
          </li>
        {/each}
      </ul>
    </div>
  </section>
{/if}

<style>
  .watchlist {
    padding: var(--space-xl) 0;
    background: var(--color-base-100);
    border-bottom: 1px solid var(--color-base-300);
  }
  .watchlist-head {
    max-width: 780px;
    margin-bottom: var(--space-md);
  }
  .watchlist-head h2 {
    color: var(--color-primary);
    font-size: var(--font-size-xl);
    margin: 0 0 var(--space-xs);
  }
  .watchlist-head p {
    color: var(--color-secondary);
    font-size: var(--font-size-sm);
    line-height: 1.5;
  }
  .watchlist-head code {
    font-family: var(--font-mono, monospace);
    font-size: var(--font-size-xs);
  }
  .watch-items { list-style: none; padding: 0; margin: 0; display: grid; gap: var(--space-xs); }
  .watch-item {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: var(--space-sm);
    padding: var(--space-xs) var(--space-sm);
    background: var(--color-base-200);
    border: 1px solid var(--color-base-300);
    border-radius: var(--radius-sm);
  }
  .watch-main { display: flex; align-items: center; gap: var(--space-sm); min-width: 0; }
  .watch-label {
    color: var(--color-primary);
    font-weight: 600;
    text-decoration: none;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .watch-kind {
    font-family: var(--font-mono, monospace);
    font-size: var(--font-size-xs);
    color: var(--color-secondary);
    text-transform: uppercase;
    letter-spacing: 0.05em;
    opacity: 0.75;
  }
  .watch-side { display: flex; align-items: center; gap: var(--space-sm); }
  .watch-feed {
    font-family: var(--font-mono, monospace);
    font-size: var(--font-size-xs);
    color: var(--color-secondary);
  }
  .watch-remove {
    background: transparent;
    border: 1px solid var(--color-base-300);
    color: var(--color-secondary);
    width: 1.75rem;
    height: 1.75rem;
    border-radius: 50%;
    cursor: pointer;
    line-height: 1;
  }
  .watch-remove:hover {
    border-color: var(--color-primary);
    color: var(--color-primary);
  }
</style>
