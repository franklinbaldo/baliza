<script lang="ts">
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { getQueryClient } from '../lib/queryClient';
  import { QUERY_KEYS } from '../lib/queryKeys';
  import { prefetchArchive } from '../lib/parquetFallback';
  import { createListQuery } from '../lib/createListQuery';
  import { fetchPublicacaoList } from '../lib/pncpPublicacao';
  import { resolveCityFromBrowserLocation } from '../lib/geo';
  import { setCity } from '../lib/cityContext.svelte';
  import { archivedContratoToInternalContract, type PNCPContract } from '../lib/pncp';
  import { formatDate, formatParticao, truncate } from '../lib/format';
  import Icon from './Icon.svelte';
  import { resolve } from '../lib/baseUrl';
  import AlertBanner from './AlertBanner.svelte';
  import EmptyState from './EmptyState.svelte';
  import Skeleton from './Skeleton.svelte';

  setQueryClientContext(getQueryClient());

  type GeoStatus = 'idle' | 'locating' | 'ready' | 'denied' | 'error';
  let geoStatus = $state<GeoStatus>('idle');
  let cityInfo = $state<{ name: string; ibge: string } | null>(null);

  interface LocalBidsView {
    contracts: PNCPContract[];
    archived?: { dataParticao: string | null };
  }

  const ibge = $derived(cityInfo?.ibge ?? '');

  $effect(() => {
    if (ibge) prefetchArchive('contratos');
  });

  const bidsQuery = createQuery(() =>
    createListQuery<LocalBidsView | null>({
      queryKey: QUERY_KEYS.localBids(ibge),
      enabled: !!ibge,
      archive: {
        column: 'codigo_ibge',
        value: ibge,
        limit: 5,
        orderByColumn: 'data_publicacao_pncp',
      },
      fetchLive: async () => {
        if (!ibge) return null;
        const contracts = await fetchPublicacaoList(
          { codigoMunicipioIbge: ibge },
          { tamanhoPagina: 10 },
        );
        return { contracts: contracts.slice(0, 5) };
      },
      buildFromArchive: ({ rows, dataParticao }) => ({
        contracts: rows.map((row) => archivedContratoToInternalContract(row)),
        archived: { dataParticao },
      }),
    }),
  );

  const data = $derived(bidsQuery.data);
  const loadingData = $derived(!!ibge && bidsQuery.isFetching);

  async function handleFindLocal() {
    geoStatus = 'locating';
    try {
      const city = await resolveCityFromBrowserLocation();
      cityInfo = { name: city.nome, ibge: city.ibge };
      // Keep the shared homepage context in sync so the hero and nav follow
      // the user's detected location without requiring a second picker step.
      setCity(city, 'storage');
      geoStatus = 'ready';
    } catch (err) {
      console.error(err);
      const code = (err as { code?: number }).code;
      geoStatus = code === 1 ? 'denied' : 'error';
    }
  }
</script>

<article>
  <header>
    <hgroup>
      <small><Icon name="map-pin" /></small>
      <h3>Radar Local</h3>
      <p>Encontre oportunidades perto de você usando geolocalização.</p>
    </hgroup>
  </header>

  {#if geoStatus === 'idle'}
    <button onclick={handleFindLocal}>Ativar Localizador</button>
  {:else if geoStatus === 'locating'}
    <p aria-busy="true">Buscando sua posição no mapa...</p>
  {:else if geoStatus === 'denied'}
    <EmptyState
      title="Acesso à localização negado"
      message="Para usar o Radar Local, permita o acesso à localização nas configurações do navegador."
      actionHref={resolve('')}
      actionLabel="Buscar manualmente"
    />
  {:else if geoStatus === 'error'}
    <EmptyState
      title="Falha na localização"
      message="Não conseguimos determinar sua localização ou o município não foi encontrado."
    />
  {:else if geoStatus === 'ready'}
    {#if loadingData}
      <Skeleton label={`Consultando PNCP para ${cityInfo?.name ?? 'seu município'}`} rows={4} />
    {:else if data}
      {#if data.archived}
        <AlertBanner
          title="Dados arquivados"
          message={`PNCP indisponível — exibindo dados arquivados (última consolidação: ${formatParticao(data.archived.dataParticao)}).`}
          level="info"
        />
      {/if}

      <header><h4>Visto recentemente em <strong>{cityInfo?.name}</strong></h4></header>

      {#if data.contracts.length === 0}
        <EmptyState
          title="Nenhuma contratação recente"
          message="O PNCP não retornou contratações recentes para este município."
        />
      {:else}
        <ul>
          {#each data.contracts as bid (bid.numeroControlePNCP)}
            <li>
              <article>
                <a href={resolve(`contratacao?id=${bid.numeroControlePNCP}`)}>
                  <header>
                    <code>#{bid.numeroControlePNCP}</code>
                    <small>{formatDate(bid.dataPublicacaoPncp)}</small>
                  </header>
                  <p>{truncate(bid.objetoContratacao, 100)}</p>
                </a>
              </article>
            </li>
          {/each}
        </ul>
      {/if}
    {:else if bidsQuery.error}
      <EmptyState
        title="Nenhuma contratação recente"
        message="O PNCP não retornou contratações recentes para este município."
      />
    {/if}
  {/if}
</article>

