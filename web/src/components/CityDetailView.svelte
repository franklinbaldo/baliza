<script lang="ts">
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { getQueryClient } from '../lib/queryClient';
  import { QUERY_KEYS } from '../lib/queryKeys';
  import { prefetchArchive } from '../lib/parquetFallback';
  import { parsePncpPublicacaoList, type PNCPContract } from '../lib/pncp';
  import { formatParticao } from '../lib/formatParticao';
  import { createDetailQuery } from '../lib/createDetailQuery';
  import type { ArchivedContrato } from '../lib/archive/schema';
  import EntityNotFound from './EntityNotFound.svelte';
  import AlertBanner from './AlertBanner.svelte';
  import EmptyState from './EmptyState.svelte';
  import StatCard from './StatCard.svelte';

  setQueryClientContext(getQueryClient());

  const { ibge: ibgeProp = "" }: { ibge?: string } = $props();

  const ibge = $derived(
    ibgeProp ||
      (typeof window !== 'undefined'
        ? new URLSearchParams(window.location.search).get('ibge') ?? ''
        : ''),
  );

  $effect(() => {
    if (ibge) prefetchArchive('contratos');
  });

  interface CityView {
    name: string;
    uf: string;
    ibge: string;
    contracts: PNCPContract[];
    archived?: { dataParticao: string | null };
  }

  function archivedRowToContract(row: ArchivedContrato): PNCPContract {
    return {
      numeroControlePNCP: row.numero_controle_pncp ?? '',
      dataPublicacaoPncp: row.data_publicacao_pncp ?? '',
      objetoContratacao: row.objeto_contrato ?? '',
      valorTotalEstimado: row.valor_global ?? row.valor_inicial ?? null,
      orgaoEntidade: {
        razaoSocial: row.razao_social_orgao ?? '',
        cnpj: row.cnpj_orgao ?? '',
      },
      unidadeOrgao: {
        nomeUnidade: row.nome_unidade ?? '',
        municipioNome: row.municipio_nome ?? '',
        ufSigla: row.uf_sigla ?? '',
        codigoMunicipioIbge: row.codigo_ibge ?? '',
      },
    };
  }

  const cityQuery = createQuery(() =>
    createDetailQuery<CityView | null>({
      queryKey: QUERY_KEYS.municipio(ibge),
      enabled: !!ibge,
      archive: {
        column: 'codigo_ibge',
        identifier: ibge,
        limit: 10,
        orderByColumn: 'data_publicacao_pncp',
      },
      fetchLive: async () => {
        if (!ibge) return null;
        const url = `https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao?codigoMunicipioIbge=${ibge}&tamanhoPagina=10`;
        const res = await fetch(url);
        if (!res.ok) throw new Error("Município não localizado ou sem publicações no PNCP.");
        const contracts = parsePncpPublicacaoList(await res.json());
        const cityName = contracts[0]?.municipio?.nomeMunicipio || contracts[0]?.unidadeOrgao?.municipioNome || "Município";
        const uf = contracts[0]?.unidadeOrgao?.ufSigla || "";
        return { name: cityName, uf, ibge, contracts };
      },
      buildFromArchive: ({ rows, dataParticao }) => {
        const contracts = rows.map(archivedRowToContract);
        const first = rows[0];
        const cityName = first?.municipio_nome ?? 'Município';
        const uf = first?.uf_sigla ?? '';
        return { name: cityName, uf, ibge, contracts, archived: { dataParticao } };
      },
    }),
  );

  const data = $derived(cityQuery.data);
  const loading = $derived(cityQuery.isFetching);
  const error = $derived(cityQuery.error instanceof Error ? cityQuery.error : null);
</script>

<div class="city-detail container">
  {#if !ibge}
    <EntityNotFound id="ausente" type="município" />
  {:else if loading}
    <div class="skeleton-wrap" aria-busy="true" aria-label="Carregando dados do município">
      <div class="skeleton skeleton-title"></div>
      <div class="skeleton skeleton-meta"></div>
      <div class="skeleton skeleton-stat"></div>
      {#each [1, 2, 3] as _, i (i)}
        <div class="skeleton skeleton-bid"></div>
      {/each}
    </div>
  {:else if error}
    <div class="error-wrap">
      <AlertBanner title="Município não encontrado" message={error.message} level="error" />
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
      <span class="type-badge">🏙️ MUNICÍPIO</span>
      <h1>{data.name} / {data.uf}</h1>
      <div class="meta-row">
        <span>Cód. IBGE: {data.ibge}</span>
        <span>Fonte: {data.archived ? 'Arquivo Parquet (IA)' : 'PNCP V1'}</span>
      </div>
    </header>

    <div class="stats-row">
      <StatCard title="Contratações Recentes" value={data.contracts.length} />
    </div>

    {#if data.contracts.length === 0}
      <EmptyState
        title="Nenhuma contratação recente"
        message="O PNCP não retornou contratações recentes para este município."
        actionHref="/baliza/"
        actionLabel="Voltar à busca"
      />
    {:else}
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
              <span class="valor">{item.valorTotalEstimado != null ? `R$ ${item.valorTotalEstimado.toLocaleString('pt-BR')}` : '—'}</span>
            </div>
          </a>
        {/each}
      </section>
    {/if}
  {/if}
</div>

<style>
  .city-detail { padding: var(--space-2xl) 0; }

  .skeleton-wrap { display: grid; gap: var(--space-md); }
  .skeleton-title { height: 2rem; width: 60%; border-radius: var(--radius-sm); }
  .skeleton-meta  { height: 1rem; width: 40%; border-radius: var(--radius-sm); }
  .skeleton-stat  { height: 5rem; width: 200px; border-radius: var(--radius-sm); }
  .skeleton-bid   { height: 5rem; border-radius: var(--radius-sm); }

  .error-wrap { display: grid; gap: var(--space-md); }
  .back-row { display: flex; }

  .hub-header { margin-bottom: var(--space-xl); border-bottom: 2px solid var(--color-base-300); padding-bottom: var(--space-md); }
  .type-badge { font-family: var(--font-mono); font-size: 0.7rem; background: var(--color-primary); color: white; padding: 2px 8px; border-radius: 4px; }
  h1 { font-size: var(--font-size-2xl); margin-top: var(--space-sm); }
  .meta-row { display: flex; gap: var(--space-md); color: var(--color-secondary); font-size: var(--font-size-sm); margin-top: 4px; }

  .stats-row { display: flex; gap: var(--space-md); margin-bottom: var(--space-xl); }

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
