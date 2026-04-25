<script lang="ts">
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { getQueryClient } from '../lib/queryClient';
  import { QUERY_KEYS } from '../lib/queryKeys';
  import { createListQuery } from '../lib/createListQuery';
  import { fetchPublicacaoList } from '../lib/pncpPublicacao';
  import { archivedContratoToInternalContract, type PNCPContract } from '../lib/pncp';
  import { formatDate, formatParticao } from '../lib/format';
  import { cityState, hydrateCityContext } from '../lib/cityContext.svelte';
  import { resolve } from '../lib/baseUrl';
  import AlertBanner from './AlertBanner.svelte';
  import EmptyState from './EmptyState.svelte';

  setQueryClientContext(getQueryClient());
  hydrateCityContext();

  const FETCH_LIMIT = 40;
  const PULSE_MODALIDADE = 6;

  interface PulseView {
    contracts: PNCPContract[];
    archived?: { dataParticao: string | null };
  }

  const ibge = $derived(cityState.ibge);

  const pulseQuery = createQuery(() =>
    createListQuery<PulseView | null>({
      queryKey: [...QUERY_KEYS.localBids(ibge), 'pulse'] as const,
      enabled: !!ibge,
      archive: {
        column: 'codigo_ibge',
        value: ibge,
        limit: FETCH_LIMIT,
        orderByColumn: 'data_publicacao_pncp',
      },
      fetchLive: async () => {
        const contracts = await fetchPublicacaoList(
          { codigoMunicipioIbge: ibge },
          {
            dateWindow: 'current-month',
            tamanhoPagina: FETCH_LIMIT,
            modalidades: [PULSE_MODALIDADE],
          },
        );
        return { contracts };
      },
      buildFromArchive: ({ rows, dataParticao }) => ({
        // Mirror the live query's modalidade scope so the section labelled
        // "pregões eletrônicos" never shows other modalities from the snapshot.
        contracts: rows
          .filter((row) => row.modalidade_id === PULSE_MODALIDADE)
          .map((row) => archivedContratoToInternalContract(row)),
        archived: { dataParticao },
      }),
    }),
  );

  const data = $derived(pulseQuery.data);
  const loading = $derived(!!ibge && pulseQuery.isFetching);
  const error = $derived(pulseQuery.error instanceof Error ? pulseQuery.error : null);

  // Show 5 most recent; the caller links through to /municipio for the rest.
  const recent = $derived(data?.contracts.slice(0, 5) ?? []);

  interface OrgRollup {
    cnpj: string;
    razaoSocial: string;
    count: number;
  }
  const topOrgaos = $derived<OrgRollup[]>(
    (() => {
      if (!data?.contracts?.length) return [];
      const by: Record<string, OrgRollup> = {};
      for (const c of data.contracts) {
        const cnpj = c.orgaoEntidade?.cnpj?.trim();
        if (!cnpj) continue;
        const prev = by[cnpj];
        if (prev) {
          prev.count += 1;
        } else {
          by[cnpj] = {
            cnpj,
            razaoSocial: c.orgaoEntidade?.razaoSocial ?? cnpj,
            count: 1,
          };
        }
      }
      return Object.values(by)
        .sort((a, b) => b.count - a.count)
        .slice(0, 5);
    })(),
  );
</script>

<section class="city-pulse" aria-labelledby="pulse-title">
  <div class="container">
    <header class="pulse-head">
      <span class="kicker">Monitorar agora</span>
      <h2 id="pulse-title">
        Mês corrente em <em>{cityState.nome}</em>
      </h2>
      <p class="pulse-sub">
        Pregões eletrônicos publicados no PNCP neste mês.
        Quando o PNCP está indisponível, Baliza mostra o último snapshot arquivado no Internet Archive.
      </p>
    </header>

    {#if !ibge}
      <EmptyState
        title="Escolha uma cidade"
        message="Selecione uma cidade no seletor acima para ver as contratações recentes."
      />
    {:else if loading && !data}
      <div class="pulse-grid" aria-busy="true" aria-label="Carregando painel da cidade">
        {#each [1, 2, 3] as _, col (col)}
          <div class="card pulse-col">
            <div class="skeleton skeleton-h"></div>
            {#each [1, 2, 3, 4] as _, r (r)}
              <div class="skeleton skeleton-row"></div>
            {/each}
          </div>
        {/each}
      </div>
    {:else if error && !data}
      <AlertBanner
        title="Não conseguimos carregar os dados"
        message={error.message}
        level="warning"
      />
    {:else if data}
      {#if data.archived}
        <AlertBanner
          title="Dados arquivados"
          message={`PNCP indisponível — exibindo snapshot de ${formatParticao(data.archived.dataParticao)} (Internet Archive).`}
          level="info"
        />
      {/if}

      <div class="pulse-grid">
        <article class="card pulse-col">
          <header class="col-head">
            <span class="col-kicker">Últimas publicações</span>
            <h3>Contratações recentes</h3>
          </header>
          {#if recent.length === 0}
            <p class="col-empty">
              Sem pregões eletrônicos publicados no mês corrente para {cityState.nome}.
            </p>
          {:else}
            <ul class="col-list">
              {#each recent as bid (bid.numeroControlePNCP)}
                <li>
                  <a href={resolve(`contratacao?id=${bid.numeroControlePNCP}`)}>
                    <span class="row-meta">
                      <span class="row-date">{formatDate(bid.dataPublicacaoPncp)}</span>
                      {#if bid.modalidadeNome}
                        <span class="row-mod">{bid.modalidadeNome}</span>
                      {/if}
                    </span>
                    <span class="row-obj">{bid.objetoContratacao}</span>
                    <span class="row-org">{bid.orgaoEntidade.razaoSocial}</span>
                  </a>
                </li>
              {/each}
            </ul>
          {/if}
          <footer class="col-foot">
            <a class="col-more" href={resolve(`municipio?ibge=${ibge}`)}>
              Ver painel completo →
            </a>
          </footer>
        </article>

        <article class="card pulse-col">
          <header class="col-head">
            <span class="col-kicker">Quem está comprando</span>
            <h3>Órgãos nas últimas publicações</h3>
          </header>
          {#if topOrgaos.length === 0}
            <p class="col-empty">Sem dados suficientes para agrupar.</p>
          {:else}
            <ul class="col-list">
              {#each topOrgaos as org (org.cnpj)}
                <li>
                  <a href={resolve(`orgao?cnpj=${org.cnpj}`)}>
                    <span class="row-count">{org.count}</span>
                    <span class="row-obj">{org.razaoSocial}</span>
                    <span class="row-org">CNPJ {org.cnpj}</span>
                  </a>
                </li>
              {/each}
            </ul>
            <p class="col-note">
              Agrupamento das publicações do mês corrente retornadas pelo PNCP.
            </p>
          {/if}
          <footer class="col-foot">
            <a class="col-more" href={resolve('explorador')}>
              Comparar no explorador →
            </a>
          </footer>
        </article>

        <article class="card pulse-col pulse-col--actions">
          <header class="col-head">
            <span class="col-kicker">Aprofundar</span>
            <h3>O que investigar a seguir</h3>
          </header>
          <ul class="action-list">
            <li>
              <a href={resolve(`municipio?ibge=${ibge}`)}>
                <strong>Histórico arquivado do município</strong>
                <span>Consulta consolidada no Parquet público</span>
              </a>
            </li>
            <li>
              <a href={resolve(`atas?objeto=${encodeURIComponent(cityState.nome)}`)}>
                <strong>Atas de registro de preços</strong>
                <span>Compromissos vigentes que podem ser aderidos</span>
              </a>
            </li>
            <li>
              <a href={resolve('explorador')}>
                <strong>SQL no explorador</strong>
                <span>Consultas abertas sobre o Parquet arquivado</span>
              </a>
            </li>
            <li>
              <a href={resolve('sobre#quarentena')}>
                <strong>Critérios de quarentena</strong>
                <span>Como Baliza sinaliza anomalias</span>
              </a>
            </li>
          </ul>
        </article>
      </div>
    {/if}
  </div>
</section>

<style>
  .city-pulse {
    padding: var(--space-8) 0;
    background:
      linear-gradient(color-mix(in srgb, var(--color-surface) 94%, transparent), color-mix(in srgb, var(--color-surface) 94%, transparent)),
      var(--color-surface);
    border-bottom: 1px solid var(--color-border);
    position: relative;
  }
  .city-pulse::before {
    content: "";
    display: block;
    width: min(14rem, 40vw);
    height: 5px;
    margin: 0 0 var(--space-6);
    background: repeating-linear-gradient(90deg, var(--color-accent) 0 36px, var(--color-ouro) 36px 50px, var(--color-azul) 50px 82px);
  }
  .pulse-head {
    max-width: 720px;
    margin-bottom: var(--space-6);
    display: grid;
    gap: var(--space-2);
  }
  .pulse-head h2 {
    font-family: var(--font-display);
    font-weight: 300;
    font-size: clamp(28px, 3vw, 40px);
    line-height: 1.15;
    letter-spacing: -0.02em;
  }
  .pulse-head h2 em {
    font-style: normal;
    color: var(--color-accent);
  }
  .pulse-sub {
    color: var(--color-text-dim);
    font-size: var(--text-md);
    line-height: 1.6;
  }
  .pulse-grid {
    display: grid;
    grid-template-columns: 1fr;
    gap: var(--space-4);
  }
  @media (min-width: 900px) {
    .pulse-grid {
      grid-template-columns: repeat(3, minmax(0, 1fr));
    }
  }
  .pulse-col {
    padding: var(--space-5);
    gap: var(--space-4);
    background: var(--color-bg);
    border: 1px solid var(--color-border);
    border-left: 5px solid var(--color-azul);
    border-radius: var(--radius-0);
    display: grid;
    grid-template-rows: auto 1fr auto;
    position: relative;
    overflow: hidden;
  }
  .pulse-col:nth-child(2) { border-left-color: var(--color-verde); }
  .pulse-col:nth-child(3) { border-left-color: var(--color-ouro); }
  .pulse-col::after {
    content: "";
    position: absolute;
    inset: 0;
    background-image:
      linear-gradient(color-mix(in srgb, var(--color-text) 5%, transparent) 1px, transparent 1px),
      linear-gradient(90deg, color-mix(in srgb, var(--color-text) 5%, transparent) 1px, transparent 1px);
    background-size: 16px 16px;
    opacity: 0;
    pointer-events: none;
    transition: opacity var(--duration-fast) var(--ease);
  }
  .pulse-col:hover {
    border-color: var(--color-azul);
    transform: translateY(-2px);
    box-shadow: var(--shadow-pool-sm);
  }
  .pulse-col:hover::after { opacity: 1; }
  .col-head {
    display: grid;
    gap: var(--space-1);
    padding-bottom: var(--space-3);
    border-bottom: 1px solid var(--color-border);
  }
  .col-kicker {
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    letter-spacing: 0.16em;
    text-transform: uppercase;
    color: var(--color-azul);
  }
  .col-head h3 {
    font-family: var(--font-display);
    font-weight: 400;
    font-size: var(--text-lg);
    line-height: 1.2;
    color: var(--color-text);
  }
  .col-list {
    list-style: none;
    margin: 0;
    padding: 0;
    display: grid;
    gap: var(--space-1);
  }
  .col-list a {
    display: grid;
    grid-template-columns: auto 1fr;
    grid-template-areas:
      'meta meta'
      'obj obj'
      'org org';
    gap: 2px 10px;
    padding: 10px 0;
    border-bottom: 1px dotted var(--color-border);
    color: inherit;
  }
  .col-list li:last-child a { border-bottom: none; }
  .col-list a:hover,
  .col-list a:focus-visible {
    color: var(--color-accent);
  }
  .row-meta {
    grid-area: meta;
    display: flex;
    gap: var(--space-2);
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    letter-spacing: 0.04em;
    color: var(--color-text-dim);
  }
  .row-date { color: var(--color-azul); }
  .row-mod {
    text-transform: uppercase;
    letter-spacing: 0.08em;
  }
  .row-count {
    grid-area: meta;
    font-family: var(--font-mono);
    font-size: var(--text-md);
    color: var(--color-accent);
    line-height: 1;
  }
  .row-obj {
    grid-area: obj;
    font-size: var(--text-sm);
    line-height: 1.35;
    display: -webkit-box;
    line-clamp: 2;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;
    overflow: hidden;
  }
  .row-org {
    grid-area: org;
    font-size: var(--text-xs);
    color: var(--color-text-dim);
  }
  .col-empty {
    font-size: var(--text-sm);
    color: var(--color-text-dim);
    margin: 0;
  }
  .col-note {
    font-size: var(--text-xs);
    color: var(--color-text-dim);
    font-style: italic;
    margin: 0;
    line-height: 1.4;
  }
  .col-foot {
    padding-top: var(--space-2);
    border-top: 1px solid var(--color-border);
  }
  .col-more {
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--color-accent);
    border-bottom: 2px solid transparent;
    padding: 2px 0;
  }
  .col-more:hover,
  .col-more:focus-visible {
    border-bottom-color: var(--color-accent);
  }
  .pulse-col--actions .col-head { border-bottom-color: var(--color-azul); }
  .action-list {
    list-style: none;
    margin: 0;
    padding: 0;
    display: grid;
    gap: var(--space-2);
  }
  .action-list a {
    display: grid;
    gap: 2px;
    padding: var(--space-3);
    border: 1px solid var(--color-border);
    background: color-mix(in srgb, var(--color-surface) 34%, transparent);
    color: inherit;
  }
  .action-list a:hover,
  .action-list a:focus-visible {
    border-color: var(--color-azul);
    background: color-mix(in srgb, var(--color-azul) 9%, transparent);
  }
  .action-list strong {
    font-family: var(--font-display);
    font-weight: 500;
    font-size: var(--text-md);
    color: var(--color-text);
  }
  .action-list span {
    font-size: var(--text-sm);
    color: var(--color-text-dim);
    line-height: 1.4;
  }
  .skeleton-h { height: 1.5rem; width: 50%; }
  .skeleton-row { height: 3.2rem; }
</style>
