<script lang="ts">
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { onMount } from 'svelte';
  import { getQueryClient } from '../lib/queryClient';
  import { QUERY_KEYS } from '../lib/queryKeys';
  import { prefetchArchive } from '../lib/parquetFallback';
  import { parsePncpContract, type PNCPContract } from '../lib/pncp';
  import { formatBRL, formatDate, formatParticao } from '../lib/format';
  import { createDetailQuery } from '../lib/createDetailQuery';
  import type { ArchivedContrato } from '../lib/archive/schema';
  import { parsePncpId, PNCP_ID_EXAMPLE } from '../lib/pncpId';
  import { getLatestParquetInfo } from '../lib/ia-manifest';
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

  // Supplier data lives in the Parquet schema (`ni_fornecedor`,
  // `nome_razao_social_fornecedor`) but not in the PNCP consulta endpoint
  // used for live fetches. We hoist it onto the view model so the
  // "Acompanhar este fornecedor" entry point can render whenever we know
  // the CNPJ, regardless of which path loaded the contract.
  interface ContractView extends PNCPContract {
    archived?: { dataParticao: string | null };
    supplierCnpj?: string | null;
    supplierName?: string | null;
  }

  function archivedRowToContract(row: ArchivedContrato, id: string): ContractView {
    return {
      numeroControlePNCP: row.numero_controle_pncp ?? id,
      dataPublicacaoPncp: row.data_publicacao_pncp ?? '',
      objetoContratacao: row.objeto_contrato ?? '',
      valorTotalEstimado: row.valor_global ?? row.valor_inicial ?? null,
      modalidadeNome: row.modalidade_nome ?? undefined,
      linkSistemaOrigem: row.link_sistema_origem ?? undefined,
      usuarioNome: row.usuario_nome ?? undefined,
      supplierCnpj: row.ni_fornecedor ?? null,
      supplierName: row.nome_razao_social_fornecedor ?? null,
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

  // Watch entry points are localStorage-backed (the @planned RSS endpoint is
  // still ahead on the roadmap). The form publishes the supplier CNPJ so the
  // future daily GitHub Action can match the watch against new contracts.
  interface WatchDraft {
    kind: 'fornecedor';
    cnpj: string;
    label: string;
    createdAt: string;
  }

  let watchFormOpen = $state(false);
  let watchLabel = $state('');
  let watchSaved = $state(false);
  let watchError = $state<string | null>(null);

  function openWatchForm() {
    if (!data?.supplierCnpj) return;
    watchFormOpen = true;
    watchLabel = data.supplierName ?? data.supplierCnpj;
    watchSaved = false;
    watchError = null;
  }

  function saveWatch(e: Event) {
    e.preventDefault();
    if (!data?.supplierCnpj) return;
    const draft: WatchDraft = {
      kind: 'fornecedor',
      cnpj: data.supplierCnpj,
      label: watchLabel.trim() || data.supplierCnpj,
      createdAt: new Date().toISOString(),
    };
    if (typeof window === 'undefined') {
      watchError = 'Armazenamento local indisponível neste ambiente.';
      watchSaved = false;
      return;
    }
    try {
      const raw = window.localStorage.getItem('baliza.watches');
      let existing: WatchDraft[] = [];
      if (raw) {
        const parsed = JSON.parse(raw);
        if (Array.isArray(parsed)) existing = parsed;
      }
      existing.push(draft);
      window.localStorage.setItem('baliza.watches', JSON.stringify(existing));
      watchSaved = true;
      watchError = null;
    } catch {
      // Storage disabled (private mode), quota exceeded, or existing JSON
      // corrupt. Surface the failure so the user knows the watch was NOT
      // persisted — the previous code silently claimed success.
      watchSaved = false;
      watchError = 'Não foi possível salvar a vigilância no navegador.';
    }
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
        message={`PNCP indisponível — exibindo dados arquivados (última consolidação: ${formatParticao(data.archived.dataParticao)}).`}
        level="info"
      />
    {/if}
    <header class="hub-header">
      <span class="type-badge">📄 CONTRATAÇÃO PÚBLICA</span>
      <h1>{data.objetoContratacao}</h1>
      <div class="meta-row">
        <span class="meta-item">ID: {data.numeroControlePNCP || id}</span>
        <span class="meta-item">📅 {formatDate(data.dataPublicacaoPncp)}</span>
        {#if effectiveSnapshot}
          <span class="meta-item snapshot-badge" data-testid="snapshot-date">
            🗄️ Dados do snapshot: <time datetime={effectiveSnapshot}>{formatParticao(effectiveSnapshot)}</time>
          </span>
        {/if}
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

      {#if data.supplierCnpj}
        <section class="card watch-card" data-testid="watch-supplier-card">
          <h3>Acompanhar este fornecedor</h3>
          <p class="muted">
            Receba atualizações quando novos contratos para
            <strong>{data.supplierName || data.supplierCnpj}</strong>
            forem publicados.
          </p>
          {#if !watchFormOpen}
            <button type="button" class="btn btn-outline" onclick={openWatchForm} data-testid="open-watch-form">
              Acompanhar este fornecedor
            </button>
          {:else}
            <form class="watch-form" onsubmit={saveWatch} data-testid="watch-form">
              <label class="watch-field">
                CNPJ do fornecedor
                <input type="text" value={data.supplierCnpj} readonly data-testid="watch-cnpj" />
              </label>
              <label class="watch-field">
                Rótulo da vigilância
                <input type="text" bind:value={watchLabel} data-testid="watch-label" />
              </label>
              <div class="watch-actions">
                <button type="submit" class="btn btn-primary btn-sm">Salvar vigilância</button>
                <button
                  type="button"
                  class="btn btn-outline btn-sm"
                  onclick={() => (watchFormOpen = false)}
                >
                  Cancelar
                </button>
              </div>
              {#if watchSaved}
                <p class="watch-confirm" role="status">
                  Vigilância salva localmente. O feed será publicado em
                  <code>/alertas/fornecedor-{data.supplierCnpj}.xml</code>.
                </p>
              {:else if watchError}
                <p class="watch-error" role="alert">{watchError}</p>
              {/if}
            </form>
          {/if}
        </section>
      {/if}

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
  .meta-row { display: flex; gap: var(--space-md); color: var(--color-secondary); font-size: var(--font-size-sm); margin-top: 8px; flex-wrap: wrap; }
  .snapshot-badge {
    padding: 2px 8px;
    border-radius: var(--radius-full);
    background: var(--color-base-200);
    border: 1px solid var(--color-base-300);
    color: var(--color-base-content);
  }

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

  .muted { color: var(--color-secondary); font-size: var(--font-size-sm); font-style: italic; margin: 0 0 var(--space-sm); }

  .watch-card { grid-column: 1 / -1; }
  .watch-form { display: grid; gap: var(--space-sm); }
  .watch-field {
    display: flex;
    flex-direction: column;
    gap: 4px;
    font-size: var(--font-size-xs);
    color: var(--color-secondary);
    font-weight: 600;
  }
  .watch-field input {
    font-family: var(--font-mono);
    padding: 6px 8px;
    border: 1px solid var(--color-base-300);
    border-radius: var(--radius-sm);
    background: var(--color-base-200);
    color: var(--color-base-content);
    font-size: var(--font-size-sm);
  }
  .watch-actions { display: flex; gap: var(--space-xs); }
  .watch-confirm {
    margin: 0;
    font-size: var(--font-size-xs);
    color: var(--color-secondary);
  }
  .watch-confirm code {
    font-family: var(--font-mono);
    color: var(--color-primary);
  }
  .watch-error {
    margin: 0;
    font-size: var(--font-size-xs);
    color: var(--color-danger, #b3261e);
  }
</style>
