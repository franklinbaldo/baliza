<script lang="ts">
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { getQueryClient } from '../lib/queryClient';
  import { QUERY_KEYS } from '../lib/queryKeys';
  import { fetchPublicacaoList } from '../lib/pncpPublicacao';
  import type { PNCPContract } from '../lib/pncp';
  import AlertBanner from './AlertBanner.svelte';
  import EmptyState from './EmptyState.svelte';

  setQueryClientContext(getQueryClient());

  const { objeto: objetoProp = '' }: { objeto?: string } = $props();

  function initialObjeto(): string {
    if (objetoProp) return objetoProp;
    if (typeof window === 'undefined') return '';
    return new URLSearchParams(window.location.search).get('objeto') ?? '';
  }

  let searchInput = $state(initialObjeto());
  let submittedObjeto = $state(initialObjeto());

  // Modality 8 = Dispensa de Licitação. Pull recent dispensa contracts from
  // PNCP and aggregate fundamentacaoLegal client-side. Fetch is wide (no
  // objeto filter at API level — PNCP doesn't accept one) then narrowed in JS.
  const query = createQuery(() => ({
    queryKey: QUERY_KEYS.dispensas(submittedObjeto),
    enabled: submittedObjeto.length >= 3,
    queryFn: async (): Promise<PNCPContract[]> => {
      const all = await fetchPublicacaoList({}, { modalidades: [8], tamanhoPagina: 50 });
      const term = submittedObjeto.toLowerCase();
      return all.filter((c) => (c.objetoContratacao ?? '').toLowerCase().includes(term));
    },
  }));

  const contracts = $derived(query.data ?? []);
  const loading = $derived(query.isFetching);
  const error = $derived(query.error as Error | null);

  // Group by fundamentacaoLegal, count, sort desc.
  type LegalCitation = { article: string; count: number; samples: PNCPContract[] };
  const citations = $derived.by((): LegalCitation[] => {
    const byArticle = new Map<string, { count: number; samples: PNCPContract[] }>();
    for (const c of contracts) {
      const article = (c.fundamentacaoLegal ?? '').trim();
      if (!article) continue;
      const slot = byArticle.get(article) ?? { count: 0, samples: [] };
      slot.count++;
      if (slot.samples.length < 3) slot.samples.push(c);
      byArticle.set(article, slot);
    }
    return [...byArticle.entries()]
      .map(([article, { count, samples }]) => ({ article, count, samples }))
      .sort((a, b) => b.count - a.count);
  });

  function handleSubmit(ev: Event) {
    ev.preventDefault();
    const term = searchInput.trim();
    submittedObjeto = term;
    if (typeof window === 'undefined') return;
    const params = new URLSearchParams(window.location.search);
    if (term) params.set('objeto', term);
    else params.delete('objeto');
    const qs = params.toString();
    window.history.replaceState({}, '', qs ? `${window.location.pathname}?${qs}` : window.location.pathname);
  }
</script>

<div class="dispensas container">
  <header class="hub-header">
    <span class="type-badge">⚖️ DISPENSAS</span>
    <h1>Dispensas — Bases Legais</h1>
    <p class="meta-row">
      Quais artigos da lei são mais citados em dispensas similares. Fonte: API PNCP, modalidade 8 (Dispensa).
    </p>
  </header>

  <form class="search-form" onsubmit={handleSubmit}>
    <label class="sr-only" for="dispensas-objeto-input">Objeto a pesquisar</label>
    <input
      id="dispensas-objeto-input"
      type="search"
      bind:value={searchInput}
      placeholder="Ex.: papel A4, manutenção de veículos..."
      aria-label="Objeto a pesquisar"
    />
    <button type="submit" class="btn btn-primary">Buscar</button>
  </form>

  {#if !submittedObjeto || submittedObjeto.length < 3}
    <EmptyState
      title="Digite o objeto da dispensa"
      message="Use ao menos 3 caracteres para ver os artigos legais mais citados em dispensas similares."
    />
  {:else if loading}
    <div class="skeleton-wrap" aria-busy="true" aria-label="Buscando dispensas">
      <div class="skeleton skeleton-bid"></div>
      <div class="skeleton skeleton-bid"></div>
    </div>
  {:else if error}
    <AlertBanner title="Não foi possível buscar dispensas" message={error.message} level="error" />
  {:else if citations.length === 0}
    <EmptyState
      title="Nenhuma base legal encontrada"
      message="Nenhuma dispensa recente do PNCP correspondeu ao objeto pesquisado."
    />
  {:else}
    <section class="legal-list" data-testid="dispensas-legal-list">
      {#each citations as cit (cit.article)}
        <article class="legal-card" data-testid="dispensas-legal-item">
          <header class="legal-head">
            <span class="legal-count">{cit.count} contrato{cit.count > 1 ? 's' : ''}</span>
            <h2 class="legal-article">{cit.article}</h2>
          </header>
          <ul class="samples">
            {#each cit.samples as sample (sample.numeroControlePNCP)}
              <li>
                <span class="agency">{sample.orgaoEntidade.razaoSocial}</span>
                <span class="objeto">{sample.objetoContratacao}</span>
              </li>
            {/each}
          </ul>
        </article>
      {/each}
    </section>
  {/if}
</div>

<style>
  .dispensas { padding: var(--space-2xl) 0; }
  .hub-header { margin-bottom: var(--space-lg); border-bottom: 2px solid var(--color-base-300); padding-bottom: var(--space-md); }
  .type-badge { font-family: var(--font-mono); font-size: 0.7rem; background: var(--color-primary); color: white; padding: 2px 8px; border-radius: 4px; }
  h1 { font-size: var(--font-size-2xl); margin-top: var(--space-sm); }
  .meta-row { color: var(--color-secondary); font-size: var(--font-size-sm); margin: 4px 0 0; }

  .search-form { display: flex; gap: var(--space-sm); margin-bottom: var(--space-lg); }
  .search-form input {
    flex: 1;
    padding: var(--space-sm) var(--space-md);
    border: 1px solid var(--color-base-300);
    border-radius: var(--radius-sm);
    font-size: var(--font-size-md);
  }
  .sr-only {
    position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px;
    overflow: hidden; clip: rect(0, 0, 0, 0); white-space: nowrap; border: 0;
  }

  .skeleton-wrap { display: grid; gap: var(--space-md); }
  .skeleton-bid { height: 5rem; border-radius: var(--radius-sm); }

  .legal-list { display: grid; gap: var(--space-md); }
  .legal-card {
    background: var(--color-base-100);
    padding: var(--space-md);
    border: 1px solid var(--color-base-300);
  }
  .legal-head { display: flex; align-items: baseline; gap: var(--space-md); margin-bottom: var(--space-sm); }
  .legal-count {
    font-family: var(--font-mono);
    font-weight: 800;
    background: var(--color-primary);
    color: white;
    padding: 2px 8px;
    border-radius: 2px;
    font-size: var(--font-size-xs);
  }
  .legal-article { font-size: var(--font-size-md); margin: 0; }
  .samples { list-style: none; padding: 0; margin: 0; display: grid; gap: var(--space-xs); font-size: var(--font-size-sm); }
  .samples li { display: flex; gap: var(--space-sm); align-items: baseline; }
  .agency { font-weight: 600; flex-shrink: 0; }
  .objeto { color: var(--color-secondary); font-size: var(--font-size-xs); }
</style>
