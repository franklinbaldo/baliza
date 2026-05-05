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
  <section aria-labelledby="watch-title">
    <div>
      <header>
        <span>Você está acompanhando</span>
        <h2 id="watch-title">
          {entries.length === 1 ? '1 item salvo' : `${entries.length} itens salvos`}
        </h2>
        <p>
          Salvos neste navegador. Baliza não envia alertas por e-mail — abra esta página
          quando quiser revisitar os órgãos e fornecedores que você marcou.
        </p>
      </header>

      <ul>
        {#each entries as entry (entry.id)}
          <li>
            <div>
              <span>{labelForType(entry.type)}</span>
              <span>{entry.filter}</span>
            </div>
            <p>{entry.label}</p>
            <div>
              <a href={linkFor(entry)}>Abrir painel →</a>
              <button
                type="button"
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

