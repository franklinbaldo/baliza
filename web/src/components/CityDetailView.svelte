<script lang="ts">
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { getQueryClient } from '../lib/queryClient';
  import { QUERY_KEYS } from '../lib/queryKeys';
  import { prefetchArchive } from '../lib/parquetFallback';
  import { archivedContratoToInternalContract, type PNCPContract } from '../lib/pncp';
  import { fetchPublicacaoList } from '../lib/pncpPublicacao';
  import { formatBRL, formatParticao } from '../lib/format';
  import { resolve } from '../lib/baseUrl';
  import { createListQuery } from '../lib/createListQuery';
  import EntityDetailLayout from './EntityDetailLayout.svelte';
  import EmptyState from './EmptyState.svelte';
  import StatCard from './StatCard.svelte';
  import ContractCard from './ContractCard.svelte';
  import PaginatedList from './PaginatedList.svelte';
  import { findMunicipalityByIbge } from '../lib/geo';

  setQueryClientContext(getQueryClient());

  const { ibge: ibgeProp = "" }: { ibge?: string } = $props();

  const ibge = $derived(
    ibgeProp ||
      (typeof window !== 'undefined'
        ? new URLSearchParams(window.location.search).get('ibge') ?? ''
        : ''),
  );

  const ibgeValid = $derived(/^\d{7}$/.test(ibge));

  const CITY_LOOKBACK_DAYS = 30;
  const CITY_END_DAYS_AGO = 0;

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

  const cityQuery = createQuery(() =>
    createListQuery<CityView | null>({
      queryKey: QUERY_KEYS.municipio(ibge, CITY_LOOKBACK_DAYS),
      enabled: !!ibge,
      archive: {
        column: 'codigo_ibge',
        value: ibge,
        limit: 10,
        orderByColumn: 'data_publicacao_pncp',
      },
      fetchLive: async () => {
        if (!ibge) return null;
        const [contracts, info] = await Promise.all([
          fetchPublicacaoList(
            { codigoMunicipioIbge: ibge },
            { sinceDays: CITY_LOOKBACK_DAYS, endDaysAgo: CITY_END_DAYS_AGO, tamanhoPagina: 50 },
          ),
          findMunicipalityByIbge(ibge),
        ]);
        const cityName = info?.nome || contracts[0]?.municipio?.nomeMunicipio || contracts[0]?.unidadeOrgao?.municipioNome || "Município";
        const uf = info?.uf || contracts[0]?.unidadeOrgao?.ufSigla || "";
        return { name: cityName, uf, ibge, contracts };
      },
      buildFromArchive: async ({ rows, dataParticao }) => {
        const contracts = rows.map((row) => archivedContratoToInternalContract(row));
        const info = await findMunicipalityByIbge(ibge);
        const first = rows[0];
        const cityName = info?.nome || (first?.municipio_nome ?? 'Município');
        const uf = info?.uf || (first?.uf_sigla ?? '');
        return { name: cityName, uf, ibge, contracts, archived: { dataParticao } };
      },
    }),
  );

  const data = $derived(cityQuery.data);
  const loading = $derived(cityQuery.isFetching);
  const error = $derived(cityQuery.error instanceof Error ? cityQuery.error : null);

  const stats = $derived.by(() => {
    if (!data?.contracts) return { count: 0, totalValue: 0 };
    let totalValue = 0;
    for (const c of data.contracts) {
      if (typeof c.valorTotalEstimado === 'number') {
        totalValue += c.valorTotalEstimado;
      }
    }
    return { count: data.contracts.length, totalValue };
  });

  const errorTitle = $derived.by(() => {
    if (!error) return 'Erro ao carregar município';
    return error.message.includes('Arquivo histórico indisponível')
      ? 'Dados do município indisponíveis no momento'
      : 'Município não encontrado';
  });
</script>

<EntityDetailLayout
  id={ibge}
  idValid={ibgeValid}
  entityType="município"
  idFormatError="Código IBGE fora do formato esperado: 7 dígitos numéricos."
  {loading}
  {error}
  {errorTitle}
  dataReady={!!data}
  archivedParticao={data?.archived?.dataParticao}
  archiveMessage={`PNCP indisponível — exibindo dados arquivados (última consolidação: ${data?.archived?.dataParticao ? formatParticao(data.archived.dataParticao) : ''}).`}
  kicker="🏙️ MUNICÍPIO"
  iconId="t3"
  title={data ? `${data.name} / ${data.uf}` : ""}
  headerStyle="margin-bottom: var(--space-xl);"
  metaTestId="city-meta"
  hasStatSkeleton={true}
>
  {#snippet metaRow()}
    {#if data}
      <span>Cód. IBGE: {data.ibge}</span>
      <span>Fonte: {data.archived ? 'Arquivo Parquet (IA)' : 'PNCP V1'}</span>
    {/if}
  {/snippet}

  {#if data}
    <div class="stats-row">
      <StatCard title="Contratações Recentes" value={stats.count} hint="Últimos 30 dias (máx. 50)" />
      <StatCard title="Valor Estimado (Amostra)" value={formatBRL(stats.totalValue)} tone="success" hint="Soma das contratações listadas" />
    </div>

    {#if data.contracts.length === 0}
      <EmptyState
        title="Nenhuma contratação recente"
        message="O PNCP não retornou contratações recentes para este município."
        actionHref={resolve('')}
        actionLabel="Voltar à busca"
      />
    {:else}
      <section class="recent-list">
        <h3>Contratações Recentes neste Município</h3>
        <PaginatedList items={data.contracts} pageSize={10}>
          {#snippet children(pageItems)}
            {#each pageItems as item (item.numeroControlePNCP)}
              <ContractCard
                id={item.numeroControlePNCP}
                date={item.dataPublicacaoPncp}
                obj={item.objetoContratacao}
                valor={item.valorTotalEstimado}
              />
            {/each}
          {/snippet}
        </PaginatedList>
      </section>
    {/if}
  {/if}
</EntityDetailLayout>

<style>
  .stats-row { display: flex; gap: var(--space-md); margin-bottom: var(--space-xl); align-items: center; flex-wrap: wrap; }

  .recent-list { display: grid; gap: var(--space-md); }
</style>
