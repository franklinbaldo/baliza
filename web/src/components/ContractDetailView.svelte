<script lang="ts">
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { getQueryClient } from '../lib/queryClient';
  import { QUERY_KEYS } from '../lib/queryKeys';
  import type { PNCPContract } from '../lib/types';
  import { queryParquetFallback, archiveErrorMessage, prefetchArchive } from '../lib/parquetFallback';
  import { parsePncpContract } from '../lib/pncp';
  import type { ArchivedContrato } from '../lib/archive/schema';
  import { parsePncpId, PNCP_ID_EXAMPLE } from '../lib/pncpId';
  import EntityNotFound from './EntityNotFound.svelte';
  import AlertBanner from './AlertBanner.svelte';

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
  });

  interface ContractView extends PNCPContract {
    archived?: { dataParticao: string | null };
  }

  function archivedRowToContract(row: ArchivedContrato, id: string): ContractView {
    return {
      numeroControlePNCP: row.numero_controle_pncp ?? id,
      dataPublicacaoPncp: row.data_publicacao_pncp ?? '',
      objetoContratacao: row.objeto_contrato ?? '',
      valorTotalEstimado: row.valor_global ?? row.valor_inicial ?? 0,
      modalidadeNome: row.modalidade_nome ?? undefined,
      linkSistemaOrigem: row.link_sistema_origem ?? undefined,
      usuarioNome: row.usuario_nome ?? undefined,
      orgaoEntidade: {
        razaoSocial: row.razao_social_orgao ?? '',
        cnpj: row.cnpj_orgao ?? '',
      },
      unidadeOrgao: {
        nomeUnidade: row.nome_unidade ?? '',
        municipioNome: row.municipio_nome ?? undefined,
        ufSigla: row.uf_sigla ?? undefined,
        codigoMunicipioIbge: row.codigo_ibge ?? undefined,
      },
    };
  }

  const contractQuery = createQuery(() => ({
    queryKey: QUERY_KEYS.contratacao(id),
    queryFn: async (): Promise<ContractView | null> => {
      if (!parsedId) return null;
      const { cnpj, sequencial, ano } = parsedId;
      const url = `https://pncp.gov.br/api/consulta/v1/orgaos/${cnpj}/contratacoes/${ano}/${sequencial}`;
      try {
        const res = await fetch(url);
        if (!res.ok) throw new Error("Contratação não localizada no PNCP.");
        return parsePncpContract(await res.json()) as ContractView;
      } catch (pncpErr) {
        const archived = await queryParquetFallback(
          'numero_controle_pncp',
          id,
          1,
        );
        if (!archived.ok) {
          throw new Error(archiveErrorMessage(archived.reason), { cause: pncpErr });
        }
        const view = archivedRowToContract(archived.rows[0], id);
        view.archived = { dataParticao: archived.dataParticao };
        return view;
      }
    },
    enabled: !!parsedId,
  }));

  const data = $derived(contractQuery.data);
  const loading = $derived(contractQuery.isFetching);
  const error = $derived(contractQuery.error instanceof Error ? contractQuery.error : null);

  function formatBRL(v: number | null | undefined): string {
    if (v == null) return '—';
    return v.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
  }

  function formatDate(iso: string | null | undefined): string {
    if (!iso) return '—';
    const d = new Date(iso);
    return isNaN(d.getTime()) ? '—' : d.toLocaleDateString('pt-BR');
  }
</script>

<div class="contract-view container">
  {#if !id}
    <EntityNotFound id="ausente" type="contratação" />
  {:else if !parsedId}
    <EntityNotFound
      id={id}
      type="contratação"
      error={`ID fora do padrão PNCP. Formato esperado: ${PNCP_ID_EXAMPLE}.`}
    />
  {:else if loading}
    <div class="skeleton-wrap" aria-busy="true" aria-label="Carregando contratação">
      <div class="skeleton skeleton-title"></div>
      <div class="skeleton skeleton-meta"></div>
      <div class="skeleton-grid">
        <div class="skeleton skeleton-card"></div>
        <div class="skeleton skeleton-card"></div>
        <div class="skeleton skeleton-card"></div>
        <div class="skeleton skeleton-card"></div>
      </div>
    </div>
  {:else if error}
    <div class="error-wrap">
      <AlertBanner title="Contratação não encontrada" message={error.message} level="error" />
      <div class="back-row">
        <a href="/baliza/" class="btn btn-outline">Voltar à busca</a>
      </div>
    </div>
  {:else if data}
    {#if data.archived}
      <AlertBanner
        title="Dados arquivados"
        message={`PNCP indisponível — exibindo dados arquivados (última consolidação: ${data.archived.dataParticao ?? 'desconhecida'}).`}
        level="info"
      />
    {/if}
    <header class="hub-header">
      <span class="type-badge">📄 CONTRATAÇÃO PÚBLICA</span>
      <h1>{data.objetoContratacao}</h1>
      <div class="meta-row">
        <span class="meta-item">ID: {data.numeroControlePNCP || id}</span>
        <span class="meta-item">📅 {formatDate(data.dataPublicacaoPncp)}</span>
      </div>
    </header>

    <div class="grid-details">
      <section class="card">
        <h3>Detalhes da Contratação</h3>
        <dl class="data-list">
          <div role="listitem"><dt>Objeto</dt><dd>{data.objetoContratacao || '—'}</dd></div>
          <div role="listitem"><dt>Nº Controle PNCP</dt><dd>{data.numeroControlePNCP || '—'}</dd></div>
          <div role="listitem"><dt>Modalidade</dt><dd>{data.modalidadeNome || '—'}</dd></div>
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
                <a href={`/baliza/orgao?cnpj=${data.orgaoEntidade.cnpj}`} class="inline-link">
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
                <a href={`/baliza/municipio?ibge=${data.unidadeOrgao.codigoMunicipioIbge}`} class="inline-link">
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
</div>

<style>
  .contract-view { padding: var(--space-2xl) 0; }

  .skeleton-wrap { display: grid; gap: var(--space-md); }
  .skeleton-title { height: 2.5rem; width: 75%; border-radius: var(--radius-sm); }
  .skeleton-meta  { height: 1rem; width: 50%; border-radius: var(--radius-sm); }
  .skeleton-grid  { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: var(--space-lg); }
  .skeleton-card  { height: 12rem; border-radius: var(--radius-sm); }

  .error-wrap { display: grid; gap: var(--space-md); }
  .back-row { display: flex; }

  .hub-header { margin-bottom: var(--space-2xl); border-bottom: 2px solid var(--color-base-300); padding-bottom: var(--space-md); }
  .type-badge { font-family: var(--font-mono); font-size: 0.7rem; background: var(--color-primary); color: white; padding: 2px 8px; border-radius: 4px; }
  h1 { font-size: var(--font-size-xl); margin-top: var(--space-sm); line-height: 1.3; }
  .meta-row { display: flex; gap: var(--space-md); color: var(--color-secondary); font-size: var(--font-size-sm); margin-top: 8px; }

  .grid-details { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: var(--space-lg); }
  .card { background: var(--color-base-100); border: 1px solid var(--color-base-300); padding: var(--space-md); border-radius: var(--radius-sm); }
  .card h3 { margin-top: 0; font-size: var(--font-size-lg); border-bottom: 1px solid var(--color-base-200); padding-bottom: 8px; margin-bottom: var(--space-md); }

  .meta-card { grid-column: 1 / -1; }

  .data-list div { display: flex; justify-content: space-between; gap: var(--space-sm); padding: 8px 0; border-bottom: 1px solid var(--color-base-200); }
  .data-list dt { font-weight: 700; color: var(--color-secondary); font-size: var(--font-size-sm); flex-shrink: 0; }
  .data-list dd {
    font-family: var(--font-mono);
    font-size: var(--font-size-sm);
    color: var(--color-primary);
    text-align: right;
    margin: 0;
    max-width: 65%;
    overflow-wrap: anywhere;
  }

  .inline-link { color: var(--color-primary); text-decoration: underline; }
  .inline-link:hover { text-decoration: none; }

  .item-list { list-style: none; padding: 0; margin: 0; display: grid; gap: var(--space-sm); }
  .item-row { padding: 8px 0; border-bottom: 1px solid var(--color-base-200); }
  .item-header { display: flex; gap: var(--space-sm); align-items: baseline; }
  .item-seq { font-family: var(--font-mono); color: var(--color-secondary); font-size: var(--font-size-xs); }
  .item-desc { font-size: var(--font-size-sm); }
  .item-meta { display: flex; justify-content: space-between; margin-top: 4px; font-family: var(--font-mono); font-size: var(--font-size-xs); color: var(--color-secondary); }
  .item-valor { color: var(--color-primary); font-weight: 700; }

  .muted { color: var(--color-secondary); font-size: var(--font-size-sm); font-style: italic; margin: 0; }
</style>
