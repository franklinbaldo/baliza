<script lang="ts">
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { getQueryClient } from '../lib/queryClient';
  import { QUERY_KEYS } from '../lib/queryKeys';
  import type { PNCPContract } from '../lib/types';
  import EntityNotFound from './EntityNotFound.svelte';
  import AlertBanner from './AlertBanner.svelte';
  import EmptyState from './EmptyState.svelte';

  setQueryClientContext(getQueryClient());

  const { cnpj = "" } = $props();

  const agencyQuery = createQuery(() => ({
    queryKey: QUERY_KEYS.orgao(cnpj),
    queryFn: async () => {
      if (!cnpj) return null;
      const url = `https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao?cnpjOrgao=${cnpj}&tamanhoPagina=10`;
      const res = await fetch(url);
      if (!res.ok) throw new Error("Órgão não localizado ou sem publicações no PNCP.");
      const pncpData = (await res.json()) as { data: PNCPContract[] };
      const contracts = pncpData.data || [];
      const agencyName = contracts[0]?.orgaoEntidade?.razaoSocial || "Órgão Público";
      return { name: agencyName, cnpj, contracts };
    },
    enabled: !!cnpj,
  }));

  const data = $derived(agencyQuery.data);
  const loading = $derived(agencyQuery.isFetching);
  const error = $derived(agencyQuery.error as Error | null);
</script>

<div class="agency-detail container">
  {#if !cnpj}
    <EntityNotFound id="ausente" type="órgão" />
  {:else if loading}
    <div class="skeleton-wrap" aria-busy="true" aria-label="Carregando dados do órgão">
      <div class="skeleton skeleton-title"></div>
      <div class="skeleton skeleton-meta"></div>
      {#each [1, 2, 3] as _, i (i)}
        <div class="skeleton skeleton-bid"></div>
      {/each}
    </div>
  {:else if error}
    <div class="error-wrap">
      <AlertBanner title="Órgão não encontrado" message={error.message} level="error" />
      <div class="back-row">
        <a href="/baliza/" class="btn btn-outline">Voltar à busca</a>
      </div>
    </div>
  {:else if data}
    <header class="hub-header">
      <span class="type-badge">🏛️ ÓRGÃO / ENTIDADE</span>
      <h1>{data.name}</h1>
      <div class="meta-row">
        <span>CNPJ: {data.cnpj}</span>
        <span>Fonte: PNCP V1</span>
      </div>
    </header>

    {#if data.contracts.length === 0}
      <EmptyState
        title="Nenhuma contratação recente"
        message="O PNCP não retornou contratações recentes para este CNPJ."
        actionHref="/baliza/"
        actionLabel="Voltar à busca"
      />
    {:else}
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
  {/if}
</div>

<style>
  .agency-detail { padding: var(--space-2xl) 0; }

  .skeleton-wrap { display: grid; gap: var(--space-md); }
  .skeleton-title { height: 2rem; width: 60%; border-radius: var(--radius-sm); }
  .skeleton-meta  { height: 1rem; width: 40%; border-radius: var(--radius-sm); }
  .skeleton-bid   { height: 5rem; border-radius: var(--radius-sm); }

  .error-wrap { display: grid; gap: var(--space-md); }
  .back-row { display: flex; }

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
</style>
