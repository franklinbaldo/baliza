<script lang="ts">
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { getQueryClient } from '../lib/queryClient';
  import type { PNCPContract } from '../lib/types';
  import EntityNotFound from './EntityNotFound.svelte';

  setQueryClientContext(getQueryClient());

  const { id = "" } = $props();

  const contractQuery = createQuery(() => ({
    queryKey: ['contratacao', id],
    queryFn: async () => {
      if (!id) return null;
      try {
        const parts = id.split("-");
        if (parts.length < 4) throw new Error("ID de contrato inválido.");
        const cnpj = parts[0];
        const ano = parts[1];
        const sequencial = parts[2];
        
        const url = `https://pncp.gov.br/api/consulta/v1/orgaos/${cnpj}/contratacoes/${ano}/${sequencial}`;
        const res = await fetch(url);
        if (!res.ok) throw new Error("Contratação não localizada no PNCP.");
        return (await res.json()) as PNCPContract;
      } catch (err: unknown) {
        throw new Error(err instanceof Error ? err.message : "Erro ao consultar contrato.", { cause: err });
      }
    },
    enabled: !!id
  }));

  const data = $derived(contractQuery.data);
  const loading = $derived(contractQuery.isFetching);
  const error = $derived(contractQuery.error instanceof Error ? contractQuery.error : null);
</script>

<div class="contract-view container">
  {#if !id}
    <EntityNotFound id="ausente" type="contratação" />
  {:else if loading}
    <div class="loader">
      <div class="spinner"></div>
      <p>Consultando PNCP para o ID {id}...</p>
    </div>
  {:else if error}
    <EntityNotFound id={id} type="contratação" error={error.message} />
  {:else if data}
    <header class="hub-header">
      <span class="type-badge">📄 CONTRATAÇÃO PÚBLICA</span>
      <h1>{data.objetoContratacao}</h1>
      <div class="meta-row">
        <span class="meta-item">ID: {id}</span>
        <span class="meta-item">📅 {new Date(data.dataPublicacaoPncp).toLocaleDateString('pt-BR')}</span>
      </div>
    </header>

    <div class="grid-details">
      <section class="card">
        <h3>Resumo Executivo</h3>
        <dl class="data-list">
          <div role="listitem"><dt>Modalidade</dt><dd>{data.modalidadeNome || 'Não informada'}</dd></div>
          <div role="listitem"><dt>Situação</dt><dd>{data.situacaoNome || 'Desconhecida'}</dd></div>
          <div role="listitem"><dt>CNPJ Comprador</dt><dd>{data.orgaoEntidade?.cnpj}</dd></div>
        </dl>
      </section>

      <section class="card">
        <h3>Itens da Licitação</h3>
        <ul class="item-list">
          {#each data.itens || [] as item (item.sequencialItem)}
            <li>{item.descricao}</li>
          {/each}
        </ul>
      </section>
    </div>
  {/if}
</div>

<style>
  .contract-view { padding: var(--space-2xl) 0; }
  .hub-header { margin-bottom: var(--space-2xl); border-bottom: 2px solid var(--color-base-300); padding-bottom: var(--space-md); }
  .type-badge { font-family: var(--font-mono); font-size: 0.7rem; background: var(--color-primary); color: white; padding: 2px 8px; border-radius: 4px; }
  h1 { font-size: var(--font-size-xl); margin-top: var(--space-sm); line-height: 1.3; }
  .meta-row { display: flex; gap: var(--space-md); color: var(--color-secondary); font-size: var(--font-size-sm); margin-top: 8px; }
  
  .grid-details { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: var(--space-lg); }
  .card { background: var(--color-base-100); border: 1px solid var(--color-base-300); padding: var(--space-md); border-radius: var(--radius-sm); }
  .card h3 { margin-top: 0; font-size: var(--font-size-lg); border-bottom: 1px solid var(--color-base-200); padding-bottom: 8px; margin-bottom: var(--space-md); }
  
  .data-list div { display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid var(--color-base-200); }
  .data-list dt { font-weight: 700; color: var(--color-secondary); font-size: var(--font-size-sm); }
  .data-list dd { font-family: var(--font-mono); font-size: var(--font-size-sm); color: var(--color-primary); }

  .item-list { list-style: none; padding: 0; }
  .item-list li { padding: 8px 0; border-bottom: 1px solid var(--color-base-200); font-size: var(--font-size-sm); }

  .loader { text-align: center; padding: var(--space-3xl) 0; }
  .spinner { margin: 0 auto var(--space-md); width: 40px; height: 40px; border: 4px solid var(--color-base-300); border-top-color: var(--color-primary); border-radius: 50%; animation: spin 1s linear infinite; }
  @keyframes spin { to { transform: rotate(360deg); } }
</style>
