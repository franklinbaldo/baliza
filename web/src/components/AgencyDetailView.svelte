<script lang="ts">
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { getQueryClient } from '../lib/queryClient';
  import type { PNCPContract, PNCPAgency } from '../lib/types';
  import EntityNotFound from './EntityNotFound.svelte';

  setQueryClientContext(getQueryClient());

  const { cnpj = "" } = $props();

  const agencyQuery = createQuery(() => ({
    queryKey: ['orgao', cnpj],
    queryFn: async () => {
      if (!cnpj) return null;
      try {
        const url = `https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao?cnpjOrgao=${cnpj}&tamanhoPagina=10`;
        const res = await fetch(url);
        if (!res.ok) throw new Error("Órgão não localizado ou sem publicações.");
        const pncpData = (await res.json()) as { data: PNCPContract[] };
        
        if (!pncpData.data || pncpData.data.length === 0) {
          throw new Error("Nenhuma contratação recente encontrada para este CNPJ no PNCP.");
        }

        const agencyName = pncpData.data[0].orgaoEntidade?.razaoSocial || "Órgão Público";
        
        return { 
          name: agencyName, 
          cnpj: cnpj,
          contracts: pncpData.data || [] 
        };
      } catch (err: unknown) {
        const error = err as Error;
        throw new Error(error.message || "Erro ao consultar o órgão.", { cause: err });
      }
    },
    enabled: !!cnpj
  }));

  const data = $derived(agencyQuery.data);
  const loading = $derived(agencyQuery.isFetching);
  const error = $derived(agencyQuery.error as Error | null);
</script>

<div class="agency-detail container">
  {#if !cnpj}
    <EntityNotFound id="ausente" type="órgão" />
  {:else if loading}
    <div class="loader">
      <div class="spinner"></div>
      <p>Gerando painel institucional para o CNPJ {cnpj}...</p>
    </div>
  {:else if error}
    <EntityNotFound id={cnpj} type="órgão" error={error.message} />
  {:else if data}
    <header class="hub-header">
      <span class="type-badge">🏛️ ÓRGÃO / ENTIDADE</span>
      <h1>{data.name}</h1>
      <div class="meta-row">
        <span>ID: {data.cnpj}</span>
        <span>Fonte: PNCP V1</span>
      </div>
    </header>

    <section class="recent-list">
      <h3>Portfólio de Contratações Recentes</h3>
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
  .agency-detail { padding: var(--space-2xl) 0; }
  .hub-header { margin-bottom: var(--space-2xl); border-bottom: 2px solid var(--color-base-300); padding-bottom: var(--space-md); }
  .type-badge { font-family: var(--font-mono); font-size: 0.7rem; background: var(--color-primary); color: white; padding: 2px 8px; border-radius: 4px; }
  h1 { font-size: var(--font-size-2xl); margin-top: var(--space-sm); }
  .meta-row { display: flex; gap: var(--space-md); color: var(--color-secondary); font-size: var(--font-size-sm); margin-top: 4px; }
  
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
