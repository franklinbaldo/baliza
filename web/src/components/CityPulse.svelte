<script lang="ts">
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { getQueryClient } from '../lib/queryClient';
  import { QUERY_KEYS } from '../lib/queryKeys';
  import { prefetchArchive } from '../lib/parquetFallback';
  import { createListQuery } from '../lib/createListQuery';
  import { fetchPublicacaoList } from '../lib/pncpPublicacao';
  import type { PNCPContract } from '../lib/pncp';
  import type { ArchivedContrato } from '../lib/archive/schema';
  import { formatDate, formatParticao } from '../lib/format';
  import { cityState, hydrateCityContext } from '../lib/cityContext.svelte';
  import AlertBanner from './AlertBanner.svelte';
  import EmptyState from './EmptyState.svelte';

  const BASE = '/baliza/';

  setQueryClientContext(getQueryClient());
  hydrateCityContext();

  const SINCE_DAYS = 30;
  const FETCH_LIMIT = 40;

  interface PulseView {
    contracts: PNCPContract[];
    archived?: { dataParticao: string | null };
  }

  function archivedToContract(row: ArchivedContrato): PNCPContract {
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

  const ibge = $derived(cityState.ibge);

  $effect(() => {
    if (ibge) prefetchArchive('contratos');
  });

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
          { sinceDays: SINCE_DAYS, tamanhoPagina: FETCH_LIMIT },
        );
        return { contracts };
      },
      buildFromArchive: ({ rows, dataParticao }) => ({
        contracts: rows.map(archivedToContract),
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
        Últimos {SINCE_DAYS} dias em <em>{cityState.nome}</em>
      </h2>
      <p class="pulse-sub">
        Dados do PNCP consolidados em tempo real.
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
              Sem publicações nos últimos {SINCE_DAYS} dias para {cityState.nome}.
            </p>
          {:else}
            <ul class="col-list">
              {#each recent as bid (bid.numeroControlePNCP)}
                <li>
                  <a href={`${BASE}contratacao?id=${bid.numeroControlePNCP}`}>
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
            <a class="col-more" href={`${BASE}municipio?ibge=${ibge}`}>
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
                  <a href={`${BASE}orgao?cnpj=${org.cnpj}`}>
                    <span class="row-count">{org.count}</span>
                    <span class="row-obj">{org.razaoSocial}</span>
                    <span class="row-org">CNPJ {org.cnpj}</span>
                  </a>
                </li>
              {/each}
            </ul>
            <p class="col-note">
              Agrupamento das {FETCH_LIMIT} publicações mais recentes — para um ranking completo dos {SINCE_DAYS} dias, veja o painel da cidade.
            </p>
          {/if}
          <footer class="col-foot">
            <a class="col-more" href={`${BASE}explorador`}>
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
              <a href={`${BASE}municipio?ibge=${ibge}&dias=90`}>
                <strong>Contratações dos últimos 90 dias</strong>
                <span>Panorama trimestral do município</span>
              </a>
            </li>
            <li>
              <a href={`${BASE}atas?objeto=${encodeURIComponent(cityState.nome)}`}>
                <strong>Atas de registro de preços</strong>
                <span>Compromissos vigentes que podem ser aderidos</span>
              </a>
            </li>
            <li>
              <a href={`${BASE}explorador`}>
                <strong>SQL no explorador</strong>
                <span>Consultas abertas sobre o Parquet arquivado</span>
              </a>
            </li>
            <li>
              <a href={`${BASE}sobre#quarentena`}>
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
    background: var(--neutral-50);
    border-bottom: 1px solid var(--neutral-100);
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
    color: var(--bulcao-accent);
  }
  .pulse-sub {
    color: var(--neutral-500);
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
    background: var(--neutral-0);
    border: 1px solid var(--neutral-100);
    display: grid;
    grid-template-rows: auto 1fr auto;
  }
  .pulse-col:hover {
    border-color: var(--neutral-200);
    transform: none;
    box-shadow: none;
  }
  .col-head {
    display: grid;
    gap: var(--space-1);
    padding-bottom: var(--space-3);
    border-bottom: 1px solid var(--neutral-100);
  }
  .col-kicker {
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    letter-spacing: 0.16em;
    text-transform: uppercase;
    color: var(--bulcao-support);
  }
  .col-head h3 {
    font-family: var(--font-display);
    font-weight: 400;
    font-size: var(--text-lg);
    line-height: 1.2;
    color: var(--bulcao-fg);
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
    border-bottom: 1px dotted var(--neutral-100);
    color: inherit;
  }
  .col-list li:last-child a { border-bottom: none; }
  .col-list a:hover {
    color: var(--bulcao-accent);
  }
  .row-meta {
    grid-area: meta;
    display: flex;
    gap: var(--space-2);
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    letter-spacing: 0.04em;
    color: var(--neutral-500);
  }
  .row-date { color: var(--bulcao-support); }
  .row-mod {
    text-transform: uppercase;
    letter-spacing: 0.08em;
  }
  .row-count {
    grid-area: meta;
    font-family: var(--font-mono);
    font-size: var(--text-md);
    color: var(--bulcao-accent);
    line-height: 1;
  }
  .row-obj {
    grid-area: obj;
    font-size: var(--text-sm);
    line-height: 1.35;
    display: -webkit-box;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;
    overflow: hidden;
  }
  .row-org {
    grid-area: org;
    font-size: var(--text-xs);
    color: var(--neutral-500);
  }
  .col-empty {
    font-size: var(--text-sm);
    color: var(--neutral-500);
    margin: 0;
  }
  .col-note {
    font-size: var(--text-xs);
    color: var(--neutral-500);
    font-style: italic;
    margin: 0;
    line-height: 1.4;
  }
  .col-foot {
    padding-top: var(--space-2);
    border-top: 1px solid var(--neutral-100);
  }
  .col-more {
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--bulcao-accent);
    border-bottom: 2px solid transparent;
    padding: 2px 0;
  }
  .col-more:hover {
    border-bottom-color: var(--bulcao-accent);
  }
  .pulse-col--actions .col-head { border-bottom-color: var(--bulcao-support); }
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
    border: 1px solid var(--neutral-100);
    color: inherit;
  }
  .action-list a:hover {
    border-color: var(--bulcao-support);
    background: var(--neutral-0);
  }
  .action-list strong {
    font-family: var(--font-display);
    font-weight: 500;
    font-size: var(--text-md);
    color: var(--bulcao-fg);
  }
  .action-list span {
    font-size: var(--text-sm);
    color: var(--neutral-500);
    line-height: 1.4;
  }
  .skeleton-h { height: 1.5rem; width: 50%; }
  .skeleton-row { height: 3.2rem; }
</style>
