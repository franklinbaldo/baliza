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
  title={data?.objetoContratacao || ""}
>
  {#snippet metaRow()}
    {#if data}
      <small>ID: <code>{data.numeroControlePNCP || id}</code></small>
      <small>Publicação: {formatDate(data.dataPublicacaoPncp)}</small>
      {#if effectiveSnapshot}
        <small data-testid="snapshot-date">
          Snapshot: <time datetime={effectiveSnapshot} data-badge>{formatParticao(effectiveSnapshot)}</time>
        </small>
      {/if}
    {/if}
  {/snippet}

  {#if data}
    <blockquote>
      <p data-testid="plain-language-summary">
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
        <footer>
          <button
            class="outline"
            onclick={() => addWatch('supplier', data.supplierCnpj!, data.supplierName ?? data.supplierCnpj!)}
            disabled={isSupplierWatched}
            aria-pressed={isSupplierWatched}
          >
            {isSupplierWatched ? '✓ Acompanhando' : 'Acompanhar este fornecedor'}
          </button>
        </footer>
      {/if}
    </blockquote>

    <div class="grid">
      <section>
        <h3>Detalhes da Contratação</h3>
        <dl>
          <div role="listitem"><dt>Objeto</dt><dd>{data.objetoContratacao || '—'}</dd></div>
          <div role="listitem"><dt>Nº Controle PNCP</dt><dd>{data.numeroControlePNCP || '—'}</dd></div>
          <div role="listitem"><dt>Modalidade</dt><dd>{#if data.modalidadeNome}<Glossary term={data.modalidadeNome}>{data.modalidadeNome}</Glossary>{:else}—{/if}</dd></div>
          <div role="listitem"><dt>Situação</dt><dd>{data.situacaoNome || '—'}</dd></div>
          <div role="listitem"><dt>Publicação</dt><dd>{formatDate(data.dataPublicacaoPncp)}</dd></div>
          <div role="listitem"><dt>Abertura de Propostas</dt><dd>{formatDate(data.dataAberturaProposta)}</dd></div>
          <div role="listitem"><dt>Encerramento de Propostas</dt><dd>{formatDate(data.dataEncerramentoProposta)}</dd></div>
        </dl>
      </section>

      <section>
        <h3>Órgão Responsável</h3>
        <dl>
          <div role="listitem">
            <dt>Razão Social</dt>
            <dd>
              {#if data.orgaoEntidade?.cnpj}
                <a href={resolve(`orgao?cnpj=${data.orgaoEntidade.cnpj}`)}>
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
                <a href={resolve(`municipio?ibge=${data.unidadeOrgao.codigoMunicipioIbge}`)}>
                  {data.unidadeOrgao?.municipioNome || '—'}{data.unidadeOrgao?.ufSigla ? ` / ${data.unidadeOrgao.ufSigla}` : ''}
                </a>
              {:else}
                {data.unidadeOrgao?.municipioNome || '—'}{data.unidadeOrgao?.ufSigla ? ` / ${data.unidadeOrgao.ufSigla}` : ''}
              {/if}
            </dd>
          </div>
        </dl>
      </section>

      <section>
        <h3>Fornecedor Vencedor</h3>
        <dl>
          <div role="listitem">
            <dt>Razão Social</dt>
            <dd>
              {#if data.supplierCnpj}
                <a href={resolve(`fornecedor?cnpj=${data.supplierCnpj}`)}>
                  {data.supplierName || '—'}
                </a>
              {:else}
                {data.supplierName || '—'}
              {/if}
            </dd>
          </div>
          <div role="listitem"><dt>CNPJ/CPF</dt><dd>{data.supplierCnpj || '—'}</dd></div>
        </dl>
      </section>

      <section>
        <h3>Valores</h3>
        <dl>
          <div role="listitem"><dt>Valor Estimado</dt><dd>{formatBRL(data.valorTotalEstimado)}</dd></div>
          {#if data.valorTotalHomologado != null}
            <div role="listitem"><dt>Valor Homologado</dt><dd>{formatBRL(data.valorTotalHomologado)}</dd></div>
          {/if}
        </dl>
      </section>

      <section>
        <h3>Itens</h3>
        {#if data.itens && data.itens.length > 0}
          <ul>
            {#each data.itens as item (item.sequencialItem)}
              <li>
                <div>
                  <span>#{item.sequencialItem}</span>
                  <span>{item.descricao}</span>
                </div>
                <div>
                  <span>Qtd: {item.quantidade ?? '—'} {item.unidadeMedida ?? ''}</span>
                  <span>Unit.: {formatBRL(item.valorUnitarioEstimado)}</span>
                </div>
              </li>
            {/each}
          </ul>
        {:else}
          <p>Sem itens publicados para esta contratação.</p>
        {/if}
      </section>

      <section>
        <h3>Fontes e Metadados</h3>
        <dl>
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
                <a href={data.linkSistemaOrigem} target="_blank" rel="noopener noreferrer">
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

