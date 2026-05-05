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

<div>
  <header>
    <span>📊 MERCADO</span>
    <h1>Análise de Mercado</h1>
    <p>
      Top compradores, fornecedores e faixa de preço. Fonte: API PNCP.
    </p>
  </header>

  <form onsubmit={handleSubmit}>
    <label for="mercado-objeto-input">Objeto a pesquisar</label>
    <input
      id="mercado-objeto-input"
      type="search"
      bind:value={searchInput}
      placeholder="Ex.: merenda escolar..."
      aria-label="Objeto a pesquisar"
    />
    <button type="submit">Buscar</button>
  </form>

  {#if !submittedObjeto || submittedObjeto.length < 3}
    <EmptyState
      title="Digite o objeto a pesquisar"
      message="Use ao menos 3 caracteres para ver o panorama do mercado."
    />
  {:else if loading}
    <div aria-busy="true" aria-label="Buscando mercado">
      <div></div>
      <div></div>
    </div>
  {:else if error}
    <AlertBanner title="Não foi possível buscar mercado" message={error.message} level="error" />
  {:else if contracts.length === 0}
    <EmptyState
      title="Nenhum dado de mercado encontrado"
      message="Nenhum contrato recente do PNCP correspondeu ao objeto pesquisado."
    />
  {:else}
    <div>
      <div data-testid="mercado-count">
        <span>Total de contratos</span>
        <span>{contracts.length}</span>
      </div>
      <div data-testid="mercado-price-range">
        <span>Faixa de preço</span>
        <span>
          {#if priceRange.min !== null && priceRange.max !== null}
            {formatBrl(priceRange.min)} — {formatBrl(priceRange.max)}
          {:else}
            N/D
          {/if}
        </span>
      </div>
    </div>
    <div>
      <section data-testid="mercado-top-buyers">
        <h2>Top Compradores</h2>
        <ul>
          {#each topBuyers as buyer (buyer.razaoSocial)}
            <li>{buyer.razaoSocial} ({buyer.count})</li>
          {/each}
        </ul>
      </section>
      <section data-testid="mercado-top-suppliers">
        <h2>Top Fornecedores</h2>
        <ul>
          {#each topSuppliers as supplier (supplier.name)}
            <li>{supplier.name} ({supplier.count})</li>
          {/each}
        </ul>
      </section>
    </div>
    <div>
        <a href={resolve(`atas?objeto=${submittedObjeto}`)}>Ver pesquisa de preços</a>
    </div>
  {/if}
</div>

