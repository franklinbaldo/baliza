<script lang="ts">
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { getQueryClient } from '../lib/queryClient';
  import { QUERY_KEYS } from '../lib/queryKeys';
  import { fetchDispensaPagesForObjeto } from '../lib/pncpPublicacao';
  import type { PNCPContract } from '../lib/pncp';
  import AlertBanner from './AlertBanner.svelte';
  import EmptyState from './EmptyState.svelte';
  import HubHeader from './HubHeader.svelte';
  import Skeleton from './Skeleton.svelte';

  setQueryClientContext(getQueryClient());

  const { objeto: objetoProp = '' }: { objeto?: string } = $props();

  function initialObjeto(): string {
    if (objetoProp) return objetoProp;
    if (typeof window === 'undefined') return '';
    return new URLSearchParams(window.location.search).get('objeto') ?? '';
  }

  let searchInput = $state(initialObjeto());
  let submittedObjeto = $state(initialObjeto());

  const query = createQuery(() => {
    // Capture the term up front so a later submit can't mutate the closure
    // mid-flight and bind the result of one fetch to a different cache key.
    const term = submittedObjeto;
    return {
      queryKey: QUERY_KEYS.dispensas(term),
      enabled: term.length >= 3,
      queryFn: () => fetchDispensaPagesForObjeto(term),
    };
  });

  const contracts = $derived(query.data ?? []);
  const loading = $derived(query.isFetching);
  const error = $derived(query.error as Error | null);

  // Group by fundamentacaoLegal. Normalize the grouping key so trivially
  // different citations of the same article (mixed casing, double-spaces,
  // trailing dots) collapse into one bucket; preserve the first observed
  // form for display.
  type LegalCitation = { article: string; count: number; samples: PNCPContract[] };
  const citations = $derived.by((): LegalCitation[] => {
    const byKey: Record<string, { article: string; count: number; samples: PNCPContract[] }> = {};
    for (const c of contracts) {
      const article = (c.fundamentacaoLegal ?? '').trim();
      if (!article) continue;
      const key = article.toLowerCase().replace(/\s+/g, ' ');
      const slot = byKey[key] ?? { article, count: 0, samples: [] };
      slot.count++;
      if (slot.samples.length < 3) slot.samples.push(c);
      byKey[key] = slot;
    }
    return Object.values(byKey).sort((a, b) => b.count - a.count);
  });

  function handleSubmit(ev: Event) {
    ev.preventDefault();
    const term = searchInput.trim();
    submittedObjeto = term;
    if (typeof window === 'undefined') return;
    // eslint-disable-next-line svelte/prefer-svelte-reactivity
    const params = new URLSearchParams(window.location.search);
    if (term) params.set('objeto', term);
    else params.delete('objeto');
    const qs = params.toString();
    window.history.replaceState({}, '', qs ? `${window.location.pathname}?${qs}` : window.location.pathname);
  }
</script>

<section>
  <HubHeader kicker="⚖️ Dispensas" title="Dispensas — Bases Legais">
    {#snippet lede()}
      <p>Quais artigos da lei são mais citados em dispensas similares. Fonte: API PNCP, modalidade 8 (Dispensa).</p>
    {/snippet}
  </HubHeader>

  <form role="search" onsubmit={handleSubmit}>
    <label for="dispensas-objeto-input" class="sr-only">Objeto a pesquisar</label>
    <div role="group" aria-label="Buscar dispensas">
      <input id="dispensas-objeto-input" type="search" bind:value={searchInput} placeholder="Ex.: papel A4, manutenção de veículos..." aria-label="Objeto a pesquisar" />
      <button type="submit">Buscar</button>
    </div>
  </form>

  {#if !submittedObjeto || submittedObjeto.length < 3}
    <EmptyState title="Digite o objeto da dispensa" message="Use ao menos 3 caracteres para ver os artigos legais mais citados em dispensas similares." />
  {:else if loading}
    <Skeleton label="Buscando dispensas" />
  {:else if error}
    <AlertBanner title="Não foi possível buscar dispensas" message={error.message} level="error" />
  {:else if citations.length === 0}
    <EmptyState title="Nenhuma base legal encontrada" message="Nenhuma dispensa recente do PNCP correspondeu ao objeto pesquisado." />
  {:else}
    <section data-testid="dispensas-legal-list">
      {#each citations as cit (cit.article)}
        <article data-testid="dispensas-legal-item">
          <header>
            <small data-badge>{cit.count} contrato{cit.count > 1 ? 's' : ''}</small>
            <h2><code>{cit.article}</code></h2>
          </header>
          <ul>
            {#each cit.samples as sample (sample.numeroControlePNCP)}
              <li>
                <strong>{sample.orgaoEntidade.razaoSocial}</strong>
                <p>{sample.objetoContratacao}</p>
              </li>
            {/each}
          </ul>
        </article>
      {/each}
    </section>
  {/if}
</section>

