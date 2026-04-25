<script lang="ts">
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { onMount } from 'svelte';
  import { getQueryClient } from '../lib/queryClient';
  import { QUERY_KEYS } from '../lib/queryKeys';
  import { prefetchArchive } from '../lib/parquetFallback';
  import {
    archivedContratoToInternalContract,
    parsePncpContract,
    type PNCPContract,
  } from '../lib/pncp';
  import { formatBRL, formatDate, formatParticao } from '../lib/format';
  import { createDetailQuery } from '../lib/createDetailQuery';
  import type { ArchivedContrato } from '../lib/archive/schema';
  import { parsePncpId, PNCP_ID_EXAMPLE } from '../lib/pncpId';
  import { getLatestParquetInfo } from '../lib/ia-manifest';
  import { addWatch, isWatched, hydrateWatches } from '../lib/watchStore.svelte';
  import { resolve } from '../lib/baseUrl';
  import EntityDetailLayout from './EntityDetailLayout.svelte';
  import Glossary from './Glossary.svelte';

  setQueryClientContext(getQueryClient());

  const { id: idProp = "" }: { id?: string } = $props();

  const id = $derived(
    idProp ||
      (typeof window !== 'undefined'
        ? new URLSearchParams(window.location.search).get('id') ?? ''
        : ''),
  );
  const parsedId = $derived(id ? parsePncpId(id) : null);

  $effect(() => {
    if (parsedId) prefetchArchive('contratos');
    hydrateWatches();
  });



  // Supplier data lives in the Parquet schema (`ni_fornecedor`,
  // `nome_razao_social_fornecedor`) but not in the PNCP consulta endpoint.
  // Hoisted onto the view so the plain-language summary can name the supplier
  // whenever the archive path loaded the contract.
  interface ContractView extends PNCPContract {
    archived?: { dataParticao: string | null };
    supplierCnpj?: string | null;
    supplierName?: string | null;
  }

  function archivedRowToContract(row: ArchivedContrato, id: string): ContractView {
    return {
      ...archivedContratoToInternalContract(row, id),
      supplierCnpj: row.ni_fornecedor ?? null,
      supplierName: row.nome_razao_social_fornecedor ?? null,
    };
  }

  const contractQuery = createQuery(() =>
    createDetailQuery<ContractView | null>({
      queryKey: QUERY_KEYS.contratacao(id),
      enabled: !!parsedId,
      archive: {
        column: 'numero_controle_pncp',
        identifier: id,
        limit: 1,
      },
      fetchLive: async () => {
        if (!parsedId) return null;
        const { cnpj, sequencial, ano } = parsedId;
        const url = `https://pncp.gov.br/api/consulta/v1/orgaos/${cnpj}/compras/${ano}/${sequencial}`;
        const res = await fetch(url);
        if (!res.ok) throw new Error("Contratação não localizada no PNCP.");
        return parsePncpContract(await res.json());
      },
      buildFromArchive: ({ rows, dataParticao }) => {
        const view = archivedRowToContract(rows[0], id);
        view.archived = { dataParticao };
        return view;
      },
    }),
  );

  const data = $derived(contractQuery.data);
  const isSupplierWatched = $derived(data?.supplierCnpj ? isWatched('supplier', data.supplierCnpj) : false);
  const loading = $derived(contractQuery.isFetching);
  const error = $derived(contractQuery.error instanceof Error ? contractQuery.error : null);

  // Manifest-backed snapshot date, shown in the header even when the live
  // PNCP call succeeds. Journey 4 (informed citizen) asks for data-freshness
  // surfacing on every detail page, not only on archive fallback.
  let snapshotDate = $state<string | null>(null);
  onMount(async () => {
    const info = await getLatestParquetInfo('contratos');
    snapshotDate = info?.dataParticao ?? null;
  });

  const effectiveSnapshot = $derived(
    data?.archived?.dataParticao ?? snapshotDate,
  );

</script>

<EntityDetailLayout
  id={id}
  idValid={!!parsedId}
  entityType="contratação"
  idFormatError={`ID fora do padrão PNCP. Formato esperado: ${PNCP_ID_EXAMPLE}.`}
  {loading}
  {error}
  dataReady={!!data}
  archivedParticao={data?.archived?.dataParticao}
  archiveMessage={`PNCP indisponível — exibindo dados arquivados (última consolidação: ${data?.archived?.dataParticao ? formatParticao(data.archived.dataParticao) : ''}).`}
  kicker="CONTRATAÇÃO PÚBLICA"
  iconId="t1"
  title={data?.objetoContratacao || ""}
>
  {#snippet metaRow()}
    {#if data}
      <span class="meta-item">ID: {data.numeroControlePNCP || id}</span>
      <span class="meta-item">Publicação: {formatDate(data.dataPublicacaoPncp)}</span>
      {#if effectiveSnapshot}
        <span class="meta-item snapshot-badge" data-testid="snapshot-date">
          Snapshot: <time datetime={effectiveSnapshot}>{formatParticao(effectiveSnapshot)}</time>
        </span>
      {/if}
    {/if}
  {/snippet}

  {#if data}
    <div style="display:flex; justify-content:space-between; align-items:flex-start; width: 100%; gap: var(--space-lg); margin-bottom: var(--space-xl);">
      <p class="plain-summary" data-testid="plain-language-summary" style="margin:0;">
        {#if data.orgaoEntidade?.razaoSocial}
          <strong>{data.orgaoEntidade.razaoSocial}</strong>
        {:else}
          Um órgão público
        {/if}
        contratou
        {#if data.supplierName || data.supplierCnpj}
          <strong>{data.supplierName ?? data.supplierCnpj}</strong>
        {:else}
          um fornecedor (ainda não identificado no snapshot)
        {/if}
        {#if data.valorTotalEstimado != null}
          pelo valor de <strong>{formatBRL(data.valorTotalEstimado)}</strong>
        {:else}
          com valor não informado
        {/if}
        para <strong>{data.objetoContratacao || 'um objeto não descrito'}</strong>{data.modalidadeNome ? `, via ${data.modalidadeNome}` : ''}.
      </p>
      {#if data.supplierCnpj}
        <button
          class="btn btn-outline"
          style="flex-shrink: 0;"
          onclick={() => addWatch('supplier', data.supplierCnpj!, data.supplierName ?? data.supplierCnpj!)}
          disabled={isSupplierWatched}
        >
          {isSupplierWatched ? '✓ Acompanhando' : 'Acompanhar este fornecedor'}
        </button>
      {/if}
    </div>

    <div class="grid-details">
      <section class="card">
        <h3>Detalhes da Contratação</h3>
        <dl class="data-list">
          <div role="listitem"><dt>Objeto</dt><dd>{data.objetoContratacao || '—'}</dd></div>
          <div role="listitem"><dt>Nº Controle PNCP</dt><dd>{data.numeroControlePNCP || '—'}</dd></div>
          <div role="listitem"><dt>Modalidade</dt><dd>{#if data.modalidadeNome}<Glossary term={data.modalidadeNome}>{data.modalidadeNome}</Glossary>{:else}—{/if}</dd></div>
          <div role="listitem"><dt>Situação</dt><dd>{data.situacaoNome || '—'}</dd></div>
          <div role="listitem"><dt>Publicação</dt><dd>{formatDate(data.dataPublicacaoPncp)}</dd></div>
          <div role="listitem"><dt>Abertura de Propostas</dt><dd>{formatDate(data.dataAberturaProposta)}</dd></div>
          <div role="listitem"><dt>Encerramento de Propostas</dt><dd>{formatDate(data.dataEncerramentoProposta)}</dd></div>
        </dl>
      </section>

      <section class="card">
        <h3>Órgão Responsável</h3>
        <dl class="data-list">
          <div role="listitem">
            <dt>Razão Social</dt>
            <dd>
              {#if data.orgaoEntidade?.cnpj}
                <a href={resolve(`orgao?cnpj=${data.orgaoEntidade.cnpj}`)} class="inline-link">
                  {data.orgaoEntidade.razaoSocial || '—'}
                </a>
              {:else}
                {data.orgaoEntidade?.razaoSocial || '—'}
              {/if}
            </dd>
          </div>
          <div role="listitem"><dt>CNPJ</dt><dd>{data.orgaoEntidade?.cnpj || '—'}</dd></div>
          <div role="listitem"><dt>Unidade</dt><dd>{data.unidadeOrgao?.nomeUnidade || '—'}</dd></div>
          <div role="listitem">
            <dt>Município</dt>
            <dd>
              {#if data.unidadeOrgao?.codigoMunicipioIbge}
                <a href={resolve(`municipio?ibge=${data.unidadeOrgao.codigoMunicipioIbge}`)} class="inline-link">
                  {data.unidadeOrgao?.municipioNome || '—'}{data.unidadeOrgao?.ufSigla ? ` / ${data.unidadeOrgao.ufSigla}` : ''}
                </a>
              {:else}
                {data.unidadeOrgao?.municipioNome || '—'}{data.unidadeOrgao?.ufSigla ? ` / ${data.unidadeOrgao.ufSigla}` : ''}
              {/if}
            </dd>
          </div>
        </dl>
      </section>

      <section class="card">
        <h3>Valores</h3>
        <dl class="data-list">
          <div role="listitem"><dt>Valor Estimado</dt><dd>{formatBRL(data.valorTotalEstimado)}</dd></div>
          {#if data.valorTotalHomologado != null}
            <div role="listitem"><dt>Valor Homologado</dt><dd>{formatBRL(data.valorTotalHomologado)}</dd></div>
          {/if}
        </dl>
      </section>

      <section class="card">
        <h3>Itens</h3>
        {#if data.itens && data.itens.length > 0}
          <ul class="item-list">
            {#each data.itens as item (item.sequencialItem)}
              <li class="item-row">
                <div class="item-header">
                  <span class="item-seq">#{item.sequencialItem}</span>
                  <span class="item-desc">{item.descricao}</span>
                </div>
                <div class="item-meta">
                  <span>Qtd: {item.quantidade ?? '—'} {item.unidadeMedida ?? ''}</span>
                  <span class="item-valor">Unit.: {formatBRL(item.valorUnitarioEstimado)}</span>
                </div>
              </li>
            {/each}
          </ul>
        {:else}
          <p class="muted">Sem itens publicados para esta contratação.</p>
        {/if}
      </section>

      <section class="card meta-card">
        <h3>Fontes e Metadados</h3>
        <dl class="data-list">
          <div role="listitem"><dt>Modo de Disputa</dt><dd>{data.modoDisputaNome || '—'}</dd></div>
          <div role="listitem"><dt>Publicado por</dt><dd>{data.usuarioNome || '—'}</dd></div>
          {#if effectiveSnapshot}
            <div role="listitem">
              <dt>Snapshot Parquet</dt>
              <dd>{formatParticao(effectiveSnapshot)}</dd>
            </div>
          {/if}
          <div role="listitem">
            <dt>Sistema de Origem</dt>
            <dd>
              {#if data.linkSistemaOrigem}
                <a href={data.linkSistemaOrigem} target="_blank" rel="noopener noreferrer" class="inline-link">
                  Abrir sistema externo ↗
                </a>
              {:else}
                —
              {/if}
            </dd>
          </div>
        </dl>
      </section>
    </div>
  {/if}
</EntityDetailLayout>

<style>
  .plain-summary {
    padding: var(--space-md);
    background: color-mix(in srgb, var(--color-surface-sunk) 64%, transparent);
    border-left: 5px solid var(--color-primary);
    border-radius: var(--radius-0);
    line-height: 1.6;
    color: var(--color-base-content);
    font-size: var(--font-size-md);
  }
  .plain-summary strong { color: var(--color-text); }

  :global(.snapshot-badge) {
    padding: 4px 10px;
    border-radius: var(--radius-full);
    background: color-mix(in srgb, var(--color-azul) 14%, transparent);
    border: 1px solid color-mix(in srgb, var(--color-azul) 32%, transparent);
    color: var(--color-base-content);
  }

  .grid-details { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: var(--space-lg); }
  .card {
    background: var(--color-base-100);
    border: 1px solid var(--color-base-300);
    border-left: 5px solid var(--color-azul);
    padding: var(--space-md);
    border-radius: var(--radius-0);
  }
  .card:nth-child(2) { border-left-color: var(--color-verde); }
  .card:nth-child(3) { border-left-color: var(--color-ouro); }
  .card:nth-child(4) { border-left-color: var(--color-tijolo); }
  .card h3 { margin-top: 0; font-size: var(--font-size-lg); border-bottom: 1px solid var(--color-base-200); padding-bottom: 8px; margin-bottom: var(--space-md); }

  .meta-card { grid-column: 1 / -1; }

  .data-list div { display: flex; justify-content: space-between; gap: var(--space-sm); padding: 8px 0; border-bottom: 1px solid var(--color-base-200); }
  .data-list dt { font-weight: 700; color: var(--color-secondary); font-size: var(--font-size-sm); flex-shrink: 0; }
  .data-list dd {
    font-family: var(--font-mono);
    font-size: var(--font-size-sm);
    color: var(--color-text);
    text-align: right;
    margin: 0;
    max-width: 65%;
    overflow-wrap: anywhere;
  }

  .inline-link { color: var(--color-azul); text-decoration: underline; }
  .inline-link:hover { text-decoration: none; }

  .item-list { list-style: none; padding: 0; margin: 0; display: grid; gap: var(--space-sm); }
  .item-row { padding: 8px 0; border-bottom: 1px solid var(--color-base-200); }
  .item-header { display: flex; gap: var(--space-sm); align-items: baseline; }
  .item-seq { font-family: var(--font-mono); color: var(--color-secondary); font-size: var(--font-size-xs); }
  .item-desc { font-size: var(--font-size-sm); }
  .item-meta { display: flex; justify-content: space-between; margin-top: 4px; font-family: var(--font-mono); font-size: var(--font-size-xs); color: var(--color-secondary); }
  .item-valor { color: var(--color-primary); font-weight: 700; }

  .muted { color: var(--color-secondary); font-size: var(--font-size-sm); font-style: italic; margin: 0 0 var(--space-sm); }

  @media (max-width: 720px) {
    .plain-summary {
      font-size: var(--font-size-base);
    }
    .data-list div,
    .item-meta {
      flex-direction: column;
      align-items: flex-start;
    }
    .data-list dd {
      max-width: 100%;
      text-align: left;
    }
  }
</style>
