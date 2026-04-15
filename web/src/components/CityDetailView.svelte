<script lang="ts">
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { getQueryClient } from '../lib/queryClient';
  import EntityNotFound from './EntityNotFound.svelte';

  setQueryClientContext(getQueryClient());

  const { ibge = "" } = $props();

  const cityQuery = createQuery(() => ({
    queryKey: ['municipio', ibge],
    queryFn: async () => {
      if (!ibge) return null;
      try {
        const url = `https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao?codigoMunicipioIbge=${ibge}&tamanhoPagina=10`;
        const res = await fetch(url);
        if (!res.ok) throw new Error("Município não localizado ou sem publicações.");
        const data = await res.json();
        
        if (!data.data || data.data.length === 0) {
          throw new Error("Nenhuma contratação encontrada para este município no PNCP.");
        }

        const cityName = data.data[0].municipio?.nome || "Município";
        const uf = data.data[0].municipio?.uf || "";
        
        return { 
          name: cityName, 
          uf,
          ibge,
          contracts: data.data || [] 
        };
      } catch (err: unknown) {
        throw new Error(err instanceof Error ? err.message : "Erro ao consultar o município.", { cause: err });
      }
    },
    enabled: !!ibge
  }));

  const data = $derived(cityQuery.data);
  const loading = $derived(cityQuery.isFetching);
  const error = $derived(cityQuery.error instanceof Error ? cityQuery.error : null);
</script>

<div class="city-detail container">
  {#if !ibge}
    <EntityNotFound id="ausente" type="município" />
  {:else if loading}
    <div class="loader">
      <div class="spinner"></div>
      <p>Gerando radar municipal para o código {ibge}...</p>
    </div>
  {:else if error}
    <EntityNotFound id={ibge} type="município" error={error.message} />
  {:else if data}
    <header class="hub-header">
      <span class="type-badge">🏙️ MUNICÍPIO</span>
      <h1>{data.name} / {data.uf}</h1>
      <div class="meta-row">
        <span>Cód. IBGE: {data.ibge}</span>
        <span>Fonte: PNCP V1</span>
      </div>
    </header>

    <div class="stats-row">
      <div class="stat-mini">
        <span class="stat-mini-label">Contratações Recentes</span>
        <strong>{data.contracts.length}</strong>
      </div>
    </div>

    <section class="recent-list">
      <h3>Contratações Recentes neste Município</h3>
      {#each data.contracts as item (item.numeroControlePNCP)}
        <a href={`/baliza/contratacao?id=${item.numeroControlePNCP}`} class="bid-link-card">
          <div class="bid-header">
            <span class="bid-id">{item.numeroControlePNCP}</span>
            <span class="bid-date">{new Date(item.dataPublicacaoPncp).toLocaleDateString('pt-BR')}</span>
          </div>
          <p class="bid-obj">{item.objetoContratacao.substring(0, 150)}...</p>
          <div class="bid-footer">
            <span class="valor">R$ {item.valorTotalEstimado?.toLocaleString('pt-BR')}</span>
          </div>
        </a>
      {/each}
    </section>
  {/if}
</div>

<style>
  .city-detail { padding: var(--space-2xl) 0; }
  .hub-header { margin-bottom: var(--space-xl); border-bottom: 2px solid var(--color-base-300); padding-bottom: var(--space-md); }
  .type-badge { font-family: var(--font-mono); font-size: 0.7rem; background: var(--color-primary); color: white; padding: 2px 8px; border-radius: 4px; }
  h1 { font-size: var(--font-size-2xl); margin-top: var(--space-sm); }
  .meta-row { display: flex; gap: var(--space-md); color: var(--color-secondary); font-size: var(--font-size-sm); margin-top: 4px; }
  
  .stats-row { display: flex; gap: var(--space-md); margin-bottom: var(--space-xl); }
  .stat-mini { background: var(--color-base-200); padding: var(--space-sm) var(--space-md); border-radius: var(--radius-sm); border: 1px solid var(--color-base-300); min-width: 150px; }
  .stat-mini-label { display: block; font-size: 0.65rem; color: var(--color-secondary); text-transform: uppercase; letter-spacing: 0.05em; font-weight: 700; }
  .stat-mini strong { font-size: var(--font-size-xl); color: var(--color-primary); }

  .recent-list { display: grid; gap: var(--space-md); }
  .bid-link-card { 
    display: block; text-decoration: none; color: inherit; background: var(--color-base-100); 
    padding: var(--space-md); border-radius: var(--radius-sm); border: 1px solid var(--color-base-300);
    transition: transform 0.2s, border-color 0.2s;
  }
  .bid-link-card:hover { transform: translateX(5px); border-color: var(--color-primary); }
  .bid-header { display: flex; justify-content: space-between; margin-bottom: var(--space-sm); font-size: 0.75rem; font-family: var(--font-mono); color: var(--color-secondary); }
  .bid-obj { font-size: var(--font-size-sm); line-height: 1.5; margin-bottom: var(--space-sm); }
  .bid-footer { text-align: right; font-weight: 800; color: var(--color-primary); }
  
  .loader { text-align: center; padding: var(--space-3xl) 0; color: var(--color-secondary); }
  .spinner { margin: 0 auto var(--space-md); width: 40px; height: 40px; border: 4px solid var(--color-base-300); border-top-color: var(--color-primary); border-radius: 50%; animation: spin 1s linear infinite; }
  @keyframes spin { to { transform: rotate(360deg); } }
</style>
