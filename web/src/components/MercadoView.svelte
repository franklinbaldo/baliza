<script lang="ts">
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { getQueryClient } from '../lib/queryClient';
  import { QUERY_KEYS } from '../lib/queryKeys';
  import { fetchPublicacaoPagesForObjeto } from '../lib/pncpPublicacao';

  import AlertBanner from './AlertBanner.svelte';
  import EmptyState from './EmptyState.svelte';
  import { resolve } from '../lib/baseUrl';

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
    const term = submittedObjeto;
    return {
      queryKey: QUERY_KEYS.busca(term),
      enabled: term.length >= 3,
      queryFn: () => fetchPublicacaoPagesForObjeto(term),
    };
  });

  const contracts = $derived(query.data ?? []);
  const loading = $derived(query.isFetching);
  const error = $derived(query.error as Error | null);

  const topBuyers = $derived.by(() => {
    const counts: Record<string, { razaoSocial: string; count: number }> = {};
    for (const c of contracts) {
      if (!c.orgaoEntidade?.cnpj) continue;
      const cnpj = c.orgaoEntidade.cnpj;
      if (!counts[cnpj]) counts[cnpj] = { razaoSocial: c.orgaoEntidade.razaoSocial || cnpj, count: 0 };
      counts[cnpj].count++;
    }
    return Object.values(counts).sort((a, b) => b.count - a.count).slice(0, 5);
  });

  const topSuppliers = $derived.by(() => {
    const counts: Record<string, { name: string; count: number }> = {};
    for (const c of contracts) {
      if (!c.nomeRazaoSocialFornecedor && !c.niFornecedor) continue;
      const id = (c.niFornecedor || c.nomeRazaoSocialFornecedor || 'Unknown') as string;
      const name = (c.nomeRazaoSocialFornecedor || c.niFornecedor || 'Unknown') as string;
      if (!counts[id]) counts[id] = { name, count: 0 };
      counts[id].count++;
    }
    return Object.values(counts).sort((a, b) => b.count - a.count).slice(0, 5);
  });

  const priceRange = $derived.by(() => {
    let min = Infinity;
    let max = -Infinity;
    for (const c of contracts) {
      if (typeof c.valorGlobal === 'number') {
        min = Math.min(min, c.valorGlobal);
        max = Math.max(max, c.valorGlobal);
      } else if (typeof c.valorTotalEstimado === 'number') {
         min = Math.min(min, c.valorTotalEstimado);
         max = Math.max(max, c.valorTotalEstimado);
      }
    }
    return { min: min === Infinity ? null : min, max: max === -Infinity ? null : max };
  });

  const formatBrl = (val: number) =>
    new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(val);

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

<div class="mercado container">
  <header class="hub-header">
    <span class="type-badge">📊 MERCADO</span>
    <h1>Análise de Mercado</h1>
    <p class="meta-row">
      Top compradores, fornecedores e faixa de preço. Fonte: API PNCP.
    </p>
  </header>

  <form class="search-form" onsubmit={handleSubmit}>
    <label class="sr-only" for="mercado-objeto-input">Objeto a pesquisar</label>
    <input
      id="mercado-objeto-input"
      type="search"
      bind:value={searchInput}
      placeholder="Ex.: merenda escolar..."
      aria-label="Objeto a pesquisar"
    />
    <button type="submit" class="btn btn-primary">Buscar</button>
  </form>

  {#if !submittedObjeto || submittedObjeto.length < 3}
    <EmptyState
      title="Digite o objeto a pesquisar"
      message="Use ao menos 3 caracteres para ver o panorama do mercado."
    />
  {:else if loading}
    <div class="skeleton-wrap" aria-busy="true" aria-label="Buscando mercado">
      <div class="skeleton skeleton-bid"></div>
      <div class="skeleton skeleton-bid"></div>
    </div>
  {:else if error}
    <AlertBanner title="Não foi possível buscar mercado" message={error.message} level="error" />
  {:else if contracts.length === 0}
    <EmptyState
      title="Nenhum dado de mercado encontrado"
      message="Nenhum contrato recente do PNCP correspondeu ao objeto pesquisado."
    />
  {:else}
    <div class="summary-stats">
      <div class="stat-card" data-testid="mercado-count">
        <span class="stat-label">Total de contratos</span>
        <span class="stat-value">{contracts.length}</span>
      </div>
      <div class="stat-card" data-testid="mercado-price-range">
        <span class="stat-label">Faixa de preço</span>
        <span class="stat-value">
          {#if priceRange.min !== null && priceRange.max !== null}
            {formatBrl(priceRange.min)} — {formatBrl(priceRange.max)}
          {:else}
            N/D
          {/if}
        </span>
      </div>
    </div>
    <div class="market-lists">
      <section class="market-list-section" data-testid="mercado-top-buyers">
        <h2>Top Compradores</h2>
        <ul>
          {#each topBuyers as buyer (buyer.razaoSocial)}
            <li>{buyer.razaoSocial} ({buyer.count})</li>
          {/each}
        </ul>
      </section>
      <section class="market-list-section" data-testid="mercado-top-suppliers">
        <h2>Top Fornecedores</h2>
        <ul>
          {#each topSuppliers as supplier (supplier.name)}
            <li>{supplier.name} ({supplier.count})</li>
          {/each}
        </ul>
      </section>
    </div>
    <div class="actions" style="margin-top: 2rem;">
        <a href={resolve(`atas?objeto=${submittedObjeto}`)} class="btn btn-outline">Ver pesquisa de preços</a>
    </div>
  {/if}
</div>

<style>
  .mercado { padding: var(--space-2xl) 0; }
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

  .summary-stats { display: flex; gap: 1rem; margin-bottom: 2rem; flex-wrap: wrap; }
  .stat-card {
    background: var(--color-base-100); padding: 1rem; border: 1px solid var(--color-base-300);
    flex: 1; min-width: 200px;
  }
  .stat-label { display: block; font-size: 0.8rem; color: var(--color-text-dim); text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.5rem; }
  .stat-value { font-size: 1.25rem; font-weight: bold; }

  .market-lists { display: grid; grid-template-columns: 1fr 1fr; gap: 2rem; }
  @media (max-width: 600px) { .market-lists { grid-template-columns: 1fr; } }
  .market-list-section h2 { font-size: 1.1rem; margin-bottom: 1rem; border-bottom: 1px solid var(--color-base-300); padding-bottom: 0.5rem; }
  .market-list-section ul { list-style: none; padding: 0; margin: 0; }
  .market-list-section li { padding: 0.5rem 0; border-bottom: 1px solid var(--color-base-200); font-size: 0.95rem; }
  .market-list-section li:last-child { border-bottom: none; }
</style>
