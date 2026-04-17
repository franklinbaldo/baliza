<script lang="ts">
  import { getUserCoordinates, getCityFromCoords, getIBGECode } from '../lib/geo';
  import type { PNCPContract } from '../lib/types';
  import EmptyState from './EmptyState.svelte';

  let locationStatus = $state<'idle' | 'locating' | 'loading_data' | 'ready' | 'error' | 'denied'>('idle');
  let cityInfo = $state<{ name: string; ibge: string } | null>(null);
  let bids = $state<PNCPContract[]>([]);

  async function handleFindLocal() {
    locationStatus = 'locating';
    try {
      const coords = await getUserCoordinates();
      const cityData = await getCityFromCoords(coords.latitude, coords.longitude);
      const ibge = await getIBGECode(cityData.city, cityData.state);

      if (!ibge) throw new Error('Não foi possível localizar o código IBGE para este município.');

      cityInfo = { name: cityData.city, ibge };
      locationStatus = 'loading_data';

      const url = `https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao?codigoMunicipioIbge=${ibge}&tamanhoPagina=5`;
      const res = await fetch(url);
      const data = await res.json();
      bids = data.data || [];
      locationStatus = 'ready';
    } catch (err) {
      console.error(err);
      const code = (err as { code?: number }).code;
      if (code === 1) {
        locationStatus = 'denied';
      } else {
        locationStatus = 'error';
      }
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

    {#if locationStatus === 'idle'}
      <button class="btn btn-primary" onclick={handleFindLocal}>
        Ativar Localizador
      </button>

    {:else if locationStatus === 'locating'}
      <div class="status-msg" aria-busy="true">
        <div class="spinner" aria-hidden="true"></div>
        <p>Buscando sua posição no mapa...</p>
      </div>

    {:else if locationStatus === 'loading_data'}
      <div class="status-msg" aria-busy="true">
        <div class="spinner" aria-hidden="true"></div>
        <p>Consultando PNCP para {cityInfo?.name}...</p>
      </div>

    {:else if locationStatus === 'ready'}
      <div class="results">
        <div class="results-header">
          <h4>Visto recentemente em <strong>{cityInfo?.name}</strong></h4>
        </div>

        {#if bids.length === 0}
          <EmptyState
            title="Nenhuma contratação recente"
            message="O PNCP não retornou contratações recentes para este município."
          />
        {:else}
          <div class="bids-list">
            {#each bids as bid (bid.numeroControlePNCP)}
              <a href={`/baliza/contratacao?id=${bid.numeroControlePNCP}`} class="bid-card">
                <div class="bid-meta">
                  <span class="bid-id">#{bid.numeroControlePNCP}</span>
                  <span class="bid-date">{new Date(bid.dataPublicacaoPncp).toLocaleDateString('pt-BR')}</span>
                </div>
                <p class="bid-obj">{bid.objetoContratacao.substring(0, 100)}...</p>
              </a>
            {/each}
          </div>
        {/if}
      </div>

    {:else if locationStatus === 'denied'}
      <EmptyState
        title="Acesso à localização negado"
        message="Para usar o Radar Local, permita o acesso à localização nas configurações do navegador."
        actionHref="/baliza/"
        actionLabel="Buscar manualmente"
      />

    {:else if locationStatus === 'error'}
      <EmptyState
        title="Falha na localização"
        message="Não conseguimos determinar sua localização ou o município não foi encontrado."
      />
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
