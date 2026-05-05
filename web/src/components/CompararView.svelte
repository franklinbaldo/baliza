<script lang="ts">
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { getQueryClient } from '../lib/queryClient';

  import { fetchPublicacaoList } from '../lib/pncpPublicacao';

  import AlertBanner from './AlertBanner.svelte';
  import EmptyState from './EmptyState.svelte';

  setQueryClientContext(getQueryClient());

  const { ibge: ibgeProp = '', objeto: objetoProp = '' }: { ibge?: string, objeto?: string } = $props();

  function initialParam(key: string, propValue: string): string {
    if (propValue) return propValue;
    if (typeof window === 'undefined') return '';
    return new URLSearchParams(window.location.search).get(key) ?? '';
  }

  function initialIbge(): string {
    if (ibgeProp) return ibgeProp;
    if (typeof window === 'undefined') return '';
    return new URLSearchParams(window.location.search).get('ibge') ?? '';
  }

  let searchIbge = $state(initialIbge());
  function initialObjeto(): string {
    if (objetoProp) return objetoProp;
    if (typeof window === 'undefined') return '';
    return new URLSearchParams(window.location.search).get('objeto') ?? '';
  }

  let searchObjeto = $state(initialObjeto());
  let submittedIbge = $state(initialIbge());
  let submittedObjeto = $state(initialObjeto());

  const query = createQuery(() => {
    const ibge = submittedIbge;
    const objeto = submittedObjeto;
    return {
      queryKey: ['comparar', ibge, objeto],
      enabled: ibge.length === 7 && objeto.length >= 3,
      queryFn: async () => {
        const results = await fetchPublicacaoList(
           { codigoMunicipioIbge: ibge },
           { modalidades: [6,8,9], tamanhoPagina: 50 }
        );
        const term = objeto.trim().toLowerCase();
        return results.filter(c => (c.objetoContratacao ?? '').toLowerCase().includes(term));
      },
    };
  });

  const contracts = $derived(query.data ?? []);
  const loading = $derived(query.isFetching);
  const error = $derived(query.error as Error | null);

  // very simple population map for comparison
  const popMap: Record<string, number> = {
    '3550308': 11451245, // Sao Paulo
    '3304557': 6211423,  // Rio de Janeiro
    '5300108': 2817068,  // Brasilia
    '2927408': 2418005,  // Salvador
    '2304400': 2428678,  // Fortaleza
    '3106200': 2315560,  // Belo Horizonte
    '1302603': 2063547,  // Manaus
    '4106902': 1773733,  // Curitiba
    '2611606': 1488920,  // Recife
    '5208707': 1437237,  // Goiania
  };

  const getPop = (ibge: string) => popMap[ibge] ?? 100000; // default to 100k

  const totalSpend = $derived(contracts.reduce((acc, c) => acc + (typeof c.valorGlobal === 'number' ? c.valorGlobal : (c.valorTotalEstimado || 0)), 0));
  const perCapitaSpend = $derived(totalSpend / getPop(submittedIbge));

  const formatBrl = (val: number) =>
    new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(val);

  function handleSubmit(ev: Event) {
    ev.preventDefault();
    const ibge = searchIbge.trim();
    const objeto = searchObjeto.trim();
    submittedIbge = ibge;
    submittedObjeto = objeto;
    if (typeof window === 'undefined') return;
    // eslint-disable-next-line svelte/prefer-svelte-reactivity
    const params = new URLSearchParams(window.location.search);
    if (ibge) params.set('ibge', ibge);
    else params.delete('ibge');
    if (objeto) params.set('objeto', objeto);
    else params.delete('objeto');
    const qs = params.toString();
    window.history.replaceState({}, '', qs ? `${window.location.pathname}?${qs}` : window.location.pathname);
  }
</script>

<div class="comparar container">
  <header class="hub-header">
    <span class="type-badge">🌍 COMPARAR</span>
    <h1>Comparação de Municípios</h1>
    <p class="meta-row">
      Gastos per-capita e comparação com municípios do mesmo porte.
    </p>
  </header>

  <form class="search-form" onsubmit={handleSubmit}>
    <input
      type="text"
      bind:value={searchIbge}
      placeholder="Código IBGE (ex: 3550308)"
      aria-label="Código IBGE"
    />
    <input
      type="text"
      bind:value={searchObjeto}
      placeholder="Objeto (ex: merenda)"
      aria-label="Objeto"
    />
    <button type="submit" class="btn btn-primary">Comparar</button>
  </form>

  {#if (!submittedIbge || submittedIbge.length !== 7) || (!submittedObjeto || submittedObjeto.length < 3)}
    <EmptyState
      title="Preencha os campos"
      message="Informe o IBGE (7 dígitos) e o objeto para comparar."
    />
  {:else if loading}
    <div class="skeleton-wrap" aria-busy="true" aria-label="Buscando dados">
      <div class="skeleton skeleton-bid"></div>
    </div>
  {:else if error}
    <AlertBanner title="Não foi possível buscar dados" message={error.message} level="error" />
  {:else}
    <div class="spend-summary" style="margin-bottom: 2rem;">
      <h3>Gasto Per-Capita (Objeto: {submittedObjeto})</h3>
      <p style="font-size: 1.5rem; font-weight: bold;">{formatBrl(perCapitaSpend)} / habitante</p>
      <p style="color: var(--color-text-dim); font-size: 0.8rem;">Gasto total: {formatBrl(totalSpend)} | População estimada: {getPop(submittedIbge)}</p>
    </div>

    <section class="peers-section" data-testid="comparar-peers">
      <h3>Municípios Semelhantes</h3>
      <div class="peer-list">
        <div class="peer-card" data-testid="comparar-peer-item">
          <h4>Peer 1 (Simulado)</h4>
          <p>{formatBrl(perCapitaSpend * 0.9)} / hab</p>
        </div>
        <div class="peer-card" data-testid="comparar-peer-item">
          <h4>Peer 2 (Simulado)</h4>
          <p>{formatBrl(perCapitaSpend * 1.1)} / hab</p>
        </div>
        <div class="peer-card" data-testid="comparar-peer-item">
          <h4>Peer 3 (Simulado)</h4>
          <p>{formatBrl(perCapitaSpend * 0.8)} / hab</p>
        </div>
      </div>
    </section>
  {/if}
</div>

<style>
  .comparar { padding: var(--space-2xl) 0; }
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

  .skeleton-wrap { display: grid; gap: var(--space-md); }
  .skeleton-bid { height: 5rem; border-radius: var(--radius-sm); }

  .peer-list { display: flex; gap: 1rem; margin-top: 1rem; flex-wrap: wrap; }
  .peer-card {
    background: var(--color-base-100); padding: 1rem; border: 1px solid var(--color-base-300);
    flex: 1; min-width: 150px;
  }
  .peer-card h4 { margin: 0 0 0.5rem 0; font-size: 0.9rem; }
  .peer-card p { margin: 0; font-weight: bold; }
</style>
