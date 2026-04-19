<script lang="ts">
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { getQueryClient } from '../lib/queryClient';
  import { QUERY_KEYS } from '../lib/queryKeys';
  import { prefetchArchive } from '../lib/parquetFallback';
  import { createListQuery } from '../lib/createListQuery';
  import { fetchPublicacaoList } from '../lib/pncpPublicacao';
  import { getUserCoordinates, getCityFromCoords, getIBGECode } from '../lib/geo';
  import type { PNCPContract } from '../lib/pncp';
  import type { ArchivedContrato } from '../lib/archive/schema';
  import { formatDate, formatParticao } from '../lib/format';
  import AlertBanner from './AlertBanner.svelte';
  import EmptyState from './EmptyState.svelte';

  setQueryClientContext(getQueryClient());

  type GeoStatus = 'idle' | 'locating' | 'ready' | 'denied' | 'error';
  let geoStatus = $state<GeoStatus>('idle');
  let cityInfo = $state<{ name: string; ibge: string } | null>(null);

  interface LocalBidsView {
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
        contracts: rows.map(archivedRowToContract),
        archived: { dataParticao },
      }),
    }),
  );

  const data = $derived(bidsQuery.data);
  const loadingData = $derived(!!ibge && bidsQuery.isFetching);

  async function handleFindLocal() {
    geoStatus = 'locating';
    try {
      const coords = await getUserCoordinates();
      const cityData = await getCityFromCoords(coords.latitude, coords.longitude);
      const code = await getIBGECode(cityData.city, cityData.state);
      if (!code) throw new Error('Não foi possível localizar o código IBGE para este município.');
      cityInfo = { name: cityData.city, ibge: code };
      geoStatus = 'ready';
    } catch (err) {
      console.error(err);
      const code = (err as { code?: number }).code;
      geoStatus = code === 1 ? 'denied' : 'error';
    }
  }
</script>

<section class="local-bids container">
  <div class="card modernist-card">
    <div class="card-header">
      <div class="icon-bulb">📍</div>
      <div class="title-group">
        <h3>Radar Local</h3>
        <p>Encontre oportunidades perto de você usando geolocalização.</p>
      </div>
    </div>

    {#if geoStatus === 'idle'}
      <button class="btn btn-primary" onclick={handleFindLocal}>
        Ativar Localizador
      </button>

    {:else if geoStatus === 'locating'}
      <div class="status-msg" aria-busy="true">
        <div class="spinner" aria-hidden="true"></div>
        <p>Buscando sua posição no mapa...</p>
      </div>

    {:else if geoStatus === 'denied'}
      <EmptyState
        title="Acesso à localização negado"
        message="Para usar o Radar Local, permita o acesso à localização nas configurações do navegador."
        actionHref="/baliza/"
        actionLabel="Buscar manualmente"
      />

    {:else if geoStatus === 'error'}
      <EmptyState
        title="Falha na localização"
        message="Não conseguimos determinar sua localização ou o município não foi encontrado."
      />

    {:else if geoStatus === 'ready'}
      {#if loadingData}
        <div
          class="skeleton-wrap"
          aria-busy="true"
          aria-label={`Consultando PNCP para ${cityInfo?.name ?? 'seu município'}`}
        >
          <div class="skeleton skeleton-heading"></div>
          {#each [1, 2, 3] as _, i (i)}
            <div class="skeleton skeleton-bid"></div>
          {/each}
        </div>
      {:else if data}
        {#if data.archived}
          <AlertBanner
            title="Dados arquivados"
            message={`PNCP indisponível — exibindo dados arquivados (última consolidação: ${formatParticao(data.archived.dataParticao)}).`}
            level="info"
          />
        {/if}

        <div class="results">
          <div class="results-header">
            <h4>Visto recentemente em <strong>{cityInfo?.name}</strong></h4>
          </div>

          {#if data.contracts.length === 0}
            <EmptyState
              title="Nenhuma contratação recente"
              message="O PNCP não retornou contratações recentes para este município."
            />
          {:else}
            <div class="bids-list">
              {#each data.contracts as bid (bid.numeroControlePNCP)}
                <a href={`/baliza/contratacao?id=${bid.numeroControlePNCP}`} class="bid-card">
                  <div class="bid-meta">
                    <span class="bid-id">#{bid.numeroControlePNCP}</span>
                    <span class="bid-date">{formatDate(bid.dataPublicacaoPncp)}</span>
                  </div>
                  <p class="bid-obj">{bid.objetoContratacao.substring(0, 100)}...</p>
                </a>
              {/each}
            </div>
          {/if}
        </div>
      {:else if bidsQuery.error}
        <EmptyState
          title="Nenhuma contratação recente"
          message="O PNCP não retornou contratações recentes para este município."
        />
      {/if}
    {/if}
  </div>
</section>

<style>
  .local-bids { padding: var(--space-xl) 0; }

  .modernist-card {
    background: var(--color-base-100);
    border: 1px solid var(--color-base-300);
    padding: var(--space-xl);
    border-radius: var(--radius-box);
  }

  .card-header {
    display: flex;
    gap: var(--space-md);
    margin-bottom: var(--space-lg);
  }

  .icon-bulb { font-size: 2rem; }

  .title-group h3 { font-size: var(--font-size-xl); margin: 0; }
  .title-group p  { color: var(--color-secondary); margin: 0; font-size: var(--font-size-sm); }

  .status-msg {
    display: flex;
    align-items: center;
    gap: 1rem;
    color: var(--color-secondary);
  }

  .spinner {
    width: 24px;
    height: 24px;
    border: 3px solid var(--color-base-300);
    border-top-color: var(--color-primary);
    border-radius: 50%;
    animation: spin 1s linear infinite;
    flex-shrink: 0;
  }

  @keyframes spin { to { transform: rotate(360deg); } }

  .skeleton-wrap { display: grid; gap: var(--space-sm); }
  .skeleton-heading { height: 1.25rem; width: 55%; border-radius: var(--radius-sm); }
  .skeleton-bid { height: 4.5rem; border-radius: var(--radius-sm); }

  .results-header {
    margin-bottom: var(--space-md);
    padding-bottom: 8px;
    border-bottom: 1px solid var(--color-base-200);
  }

  .bids-list { display: grid; gap: var(--space-sm); }

  .bid-card {
    display: block;
    text-decoration: none;
    color: inherit;
    padding: var(--space-md);
    background: var(--color-base-200);
    border-radius: var(--radius-sm);
    transition: background var(--transition-base);
  }

  .bid-card:hover { background: var(--color-base-300); }

  .bid-meta {
    display: flex;
    justify-content: space-between;
    margin-bottom: 4px;
  }

  .bid-id   { font-family: var(--font-mono); font-size: 0.75rem; color: var(--color-primary); }
  .bid-date { font-size: 0.75rem; color: var(--color-secondary); }
  .bid-obj  { font-size: var(--font-size-sm); line-height: 1.4; margin: 0; }
</style>
