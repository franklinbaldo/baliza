
<script lang="ts">
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { getQueryClient } from '../lib/queryClient';
  import { QUERY_KEYS } from '../lib/queryKeys';
  import { prefetchArchive } from '../lib/parquetFallback';
  import type { PNCPContract } from '../lib/pncp';
  import { fetchPublicacaoList } from '../lib/pncpPublicacao';
  import { formatBRL, formatDate, formatParticao } from '../lib/format';
  import { resolve } from '../lib/baseUrl';
  import { createListQuery } from '../lib/createListQuery';
  import type { ArchivedContrato } from '../lib/archive/schema';
  import EntityNotFound from './EntityNotFound.svelte';
  import AlertBanner from './AlertBanner.svelte';
  import EmptyState from './EmptyState.svelte';
  import LookbackWindow from './LookbackWindow.svelte';
    import { DEFAULT_SINCE_DAYS } from '../lib/pncpPublicacao';
  import { addWatch, isWatched, hydrateWatches } from '../lib/watchStore.svelte';

  setQueryClientContext(getQueryClient());

  const { cnpj: cnpjProp = "" }: { cnpj?: string } = $props();

  const cnpj = $derived(
    cnpjProp ||
      (typeof window !== 'undefined'
        ? new URLSearchParams(window.location.search).get('cnpj') ?? ''
        : ''),
  );

  const cnpjValid = $derived(/^\d{14}$/.test(cnpj));

  const ALLOWED_DIAS = new Set([30, 90, 180, 365]);

  function readDiasFromUrl(): number {
    if (typeof window === 'undefined') return DEFAULT_SINCE_DAYS;
    const raw = new URLSearchParams(window.location.search).get('dias');
    const parsed = raw ? Number(raw) : NaN;
    return ALLOWED_DIAS.has(parsed) ? parsed : DEFAULT_SINCE_DAYS;
  }

  let dias = $state<number>(readDiasFromUrl());

  function updateDias(next: number) {
    dias = next;
    if (typeof window === 'undefined') return;
    const entries = Object.fromEntries(new URLSearchParams(window.location.search));
    const params = new URLSearchParams({ ...entries, dias: String(next) });
    window.history.replaceState({}, '', `${window.location.pathname}?${params.toString()}`);
  }

  $effect(() => {
    if (cnpj) prefetchArchive('contratos');
    hydrateWatches();
  });

  const isAgencyWatched = $derived(isWatched('agency', cnpj));

  interface SupplierTally {
    name: string;
    cnpj: string;
    count: number;
  }

  interface MonthBucket {
    month: string; // ISO YYYY-MM
    label: string; // pt-BR short
    count: number;
  }

  interface AgencyView {
    name: string;
    cnpj: string;
    contracts: PNCPContract[];
    topSuppliers: SupplierTally[];
    monthly: MonthBucket[];
    archived?: { dataParticao: string | null };
  }

  function archivedRowToContract(row: ArchivedContrato): PNCPContract {
    return {
      numeroControlePNCP: row.numero_controle_pncp ?? '',
      dataPublicacaoPncp: row.data_publicacao_pncp ?? '',
      objetoContratacao: row.objeto_contrato ?? '',
      valorTotalEstimado: row.valor_global ?? row.valor_inicial ?? null,
      orgaoEntidade: {
        razaoSocial: row.razao_social_orgao ?? 'Órgão Arquivado',
        cnpj: row.cnpj_orgao ?? '',
      },
      unidadeOrgao: {
        nomeUnidade: row.nome_unidade ?? '',
      },
    };
  }

  // Top suppliers are aggregated only from the archive path because the live
  // PNCP publicacao endpoint does not return fornecedor details. Empty array
  // means "fornecedor info unavailable for this lookback"; the UI shows a
  // "dados disponíveis apenas no snapshot Parquet" hint in that case.
  function computeTopSuppliers(rows: ArchivedContrato[]): SupplierTally[] {
    // Local aggregation buffer — not reactive state, so plain Map is fine.
    // eslint-disable-next-line svelte/prefer-svelte-reactivity
    const tally = new Map<string, SupplierTally>();
    for (const row of rows) {
      const cnpjKey = row.ni_fornecedor ?? '';
      const name = row.nome_razao_social_fornecedor ?? '';
      if (!cnpjKey && !name) continue;
      const key = cnpjKey || name;
      const existing = tally.get(key);
      if (existing) {
        existing.count += 1;
      } else {
        tally.set(key, { cnpj: cnpjKey, name: name || '—', count: 1 });
      }
    }
    return [...tally.values()].sort((a, b) => b.count - a.count).slice(0, 5);
  }

  // 12-month window back from today. Buckets with zero count still appear so
  // the chart axis is continuous (gaps would hide slow periods, which is
  // exactly what the "time series" scenario wants to surface).
  function computeMonthly(contracts: Pick<PNCPContract, 'dataPublicacaoPncp'>[]): MonthBucket[] {
    const now = new Date();
    const buckets: MonthBucket[] = [];
    for (let i = 11; i >= 0; i--) {
      const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
      const month = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
      const label = d.toLocaleDateString('pt-BR', { month: 'short', year: '2-digit' });
      buckets.push({ month, label, count: 0 });
    }
    const indexByMonth = new Map(buckets.map((b, i) => [b.month, i]));
    for (const c of contracts) {
      if (!c.dataPublicacaoPncp) continue;
      // YYYY-MM-DD inputs are parsed as UTC midnight; reading the month via
      // local accessors drifts day-1 publications into the previous month
      // for viewers west of UTC. Slice the YYYY-MM prefix directly when the
      // string already starts with it; otherwise fall back to UTC accessors.
      const raw = c.dataPublicacaoPncp;
      let key: string;
      if (/^\d{4}-\d{2}/.test(raw)) {
        key = raw.slice(0, 7);
      } else {
        const d = new Date(raw);
        if (isNaN(d.getTime())) continue;
        key = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
      }
      const idx = indexByMonth.get(key);
      if (idx != null) buckets[idx].count += 1;
    }
    return buckets;
  }

  const agencyQuery = createQuery(() =>
    createListQuery<AgencyView | null>({
      queryKey: QUERY_KEYS.orgao(cnpj, dias),
      enabled: !!cnpj,
      archive: {
        column: 'cnpj_orgao',
        value: cnpj,
        // 50 keeps the archive rollup (topSuppliers + monthly chart) in
        // parity with the live branch — smaller sample sizes under-count
        // suppliers and misrank the monthly activity for any agency with
        // non-trivial history.
        limit: 50,
        orderByColumn: 'data_publicacao_pncp',
      },
      fetchLive: async () => {
        if (!cnpj) return null;
        // tamanhoPagina: 50 is PNCP's max. Without it the default of 10
        // caps the monthly rollup at 10 rows, which under-counts any agency
        // with non-trivial recent activity.
        const contracts = await fetchPublicacaoList({ cnpj }, { sinceDays: dias, tamanhoPagina: 50 });
        const agencyName = contracts[0]?.orgaoEntidade?.razaoSocial || "Órgão Público";
        return {
          name: agencyName,
          cnpj,
          contracts,
          topSuppliers: [],
          monthly: computeMonthly(contracts),
        };
      },
      buildFromArchive: ({ rows, dataParticao }) => {
        const contracts = rows.map(archivedRowToContract);
        const agencyName = contracts[0]?.orgaoEntidade?.razaoSocial || "Órgão Arquivado";
        return {
          name: agencyName,
          cnpj,
          contracts,
          topSuppliers: computeTopSuppliers(rows),
          monthly: computeMonthly(contracts),
          archived: { dataParticao },
        };
      },
    }),
  );

  const data = $derived(agencyQuery.data);
  const loading = $derived(agencyQuery.isFetching);
  const error = $derived(agencyQuery.error as Error | null);
  const maxMonthly = $derived(
    data ? Math.max(1, ...data.monthly.map((m) => m.count)) : 1,
  );
</script>

<div class="agency-detail container">
  {#if !cnpj}
    <EntityNotFound id="ausente" type="órgão" />
  {:else if !cnpjValid}
    <EntityNotFound
      id={cnpj}
      type="órgão"
      error="CNPJ fora do formato esperado: 14 dígitos numéricos, sem pontos, traços ou barras."
    />
  {:else if loading}
    <div class="skeleton-wrap" aria-busy="true" aria-label="Carregando dados do órgão">
      <div class="skeleton skeleton-title"></div>
      <div class="skeleton skeleton-meta"></div>
      {#each [1, 2, 3] as _, i (i)}
        <div class="skeleton skeleton-bid"></div>
      {/each}
    </div>
  {:else if error}
    <div class="error-wrap">
      <AlertBanner title="Órgão não encontrado" message={error.message} level="error" />
      <div class="back-row">
        <a href={resolve('')} class="btn btn-outline">Voltar à busca</a>
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
      <div style="display:flex; justify-content:space-between; align-items:flex-start; width: 100%;">
        <div>
          <span class="kicker">🏛️ ÓRGÃO / ENTIDADE</span>
          <div style="display:flex; align-items:center;">
            <svg width="32" height="32" aria-hidden="true" style="margin-right: 12px; flex-shrink: 0; fill: currentColor;"><use href="#t4"/></svg>
            <h1>{data.name}</h1>
          </div>
          <div class="meta-row">
            <span>CNPJ: {data.cnpj}</span>
            <span>Fonte: {data.archived ? 'Arquivo Parquet (IA)' : 'PNCP V1'}</span>
          </div>
        </div>
        <button
          class="btn btn-outline"
          onclick={() => addWatch('agency', data.cnpj, data.name)}
          disabled={isAgencyWatched}
        >
          {isAgencyWatched ? '✓ Acompanhando' : 'Receber alertas deste órgão'}
        </button>
      </div>
    </header>

    {#if data.contracts.length === 0}
      <EmptyState
        title="Nenhuma contratação recente"
        message="O PNCP não retornou contratações recentes para este CNPJ."
        actionHref={resolve('')}
        actionLabel="Voltar à busca"
      />
    {:else}
      <section class="rollup-grid">
        <article class="rollup-card" data-testid="top-suppliers">
          <h3>Top 5 fornecedores</h3>
          {#if data.topSuppliers.length === 0}
            <p class="muted">
              Dados de fornecedor disponíveis apenas no snapshot Parquet.
              Alterne a janela para consultar o arquivo.
            </p>
          {:else}
            <ol class="supplier-list">
              {#each data.topSuppliers as s, i (`${s.cnpj || s.name}-${i}`)}
                <li>
                  <span class="supplier-rank">#{i + 1}</span>
                  <span class="supplier-name">{s.name}</span>
                  <span class="supplier-count">{s.count}</span>
                </li>
              {/each}
            </ol>
          {/if}
        </article>

        <article class="rollup-card" data-testid="monthly-chart">
          <h3>Contratos por mês (12 meses)</h3>
          <ul class="monthly-chart" aria-label="Contratos por mês">
            {#each data.monthly as m (m.month)}
              <li>
                <span class="month-label">{m.label}</span>
                <span
                  class="month-bar"
                  style={`width: ${(m.count / maxMonthly) * 100}%`}
                  aria-hidden="true"
                ></span>
                <span class="month-count">{m.count}</span>
              </li>
            {/each}
          </ul>
        </article>
      </section>

      <section class="recent-list">
        <div class="recent-header">
          <h3>Portfólio de Contratações Recentes</h3>
          <LookbackWindow value={dias} onchange={updateDias} />
        </div>
        {#each data.contracts as item (item.numeroControlePNCP)}
          <a href={resolve(`contratacao?id=${item.numeroControlePNCP}`)} class="bid-link-card">
            <div class="bid-header">
              <span class="bid-id">{item.numeroControlePNCP}</span>
              <span class="bid-date">{formatDate(item.dataPublicacaoPncp)}</span>
            </div>
            <p class="bid-obj">{item.objetoContratacao.substring(0, 150)}...</p>
            <div class="bid-footer">
              <span class="valor">{formatBRL(item.valorTotalEstimado)}</span>
            </div>
          </a>
        {/each}
      </section>
    {/if}
  {/if}
</div>

<style>
  .agency-detail { padding: var(--space-2xl) 0; }

  .skeleton-wrap { display: grid; gap: var(--space-md); }
  .skeleton-title { height: 2rem; width: 60%; border-radius: var(--radius-sm); }
  .skeleton-meta  { height: 1rem; width: 40%; border-radius: var(--radius-sm); }
  .skeleton-bid   { height: 5rem; border-radius: var(--radius-sm); }

  .error-wrap { display: grid; gap: var(--space-md); }
  .back-row { display: flex; }

  .hub-header { margin-bottom: var(--space-2xl); border-bottom: 2px solid var(--color-base-300); padding-bottom: var(--space-md); }
  h1 { font-size: var(--font-size-2xl); margin-top: var(--space-sm); }
  .meta-row { display: flex; gap: var(--space-md); color: var(--color-secondary); font-size: var(--font-size-sm); margin-top: 4px; }

  .rollup-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    gap: var(--space-lg);
    margin-bottom: var(--space-2xl);
  }

  .rollup-card {
    background: var(--color-base-100);
    border: 1px solid var(--color-base-300);
    border-radius: var(--radius-sm);
    padding: var(--space-md);
  }
  .rollup-card h3 { margin: 0 0 var(--space-sm); font-size: var(--font-size-md); }
  .rollup-card .muted { color: var(--color-secondary); font-size: var(--font-size-sm); margin: 0; }

  .supplier-list { list-style: none; padding: 0; margin: 0; display: grid; gap: var(--space-xs); }
  .supplier-list li {
    display: grid;
    grid-template-columns: auto 1fr auto;
    gap: var(--space-sm);
    align-items: baseline;
    font-size: var(--font-size-sm);
    padding: 4px 0;
    border-bottom: 1px solid var(--color-base-200);
  }
  .supplier-list li:last-child { border-bottom: none; }
  .supplier-rank { font-family: var(--font-mono); font-weight: 700; color: var(--color-secondary); }
  .supplier-name { overflow-wrap: anywhere; }
  .supplier-count { font-family: var(--font-mono); color: var(--color-primary); font-weight: 700; }

  .monthly-chart { list-style: none; padding: 0; margin: 0; display: grid; gap: 4px; }
  .monthly-chart li {
    display: grid;
    grid-template-columns: 3rem 1fr auto;
    gap: var(--space-xs);
    align-items: center;
    font-size: var(--font-size-xs);
  }
  .month-label { font-family: var(--font-mono); color: var(--color-secondary); }
  .month-bar {
    display: block;
    height: 0.6rem;
    min-width: 2px;
    background: var(--color-primary);
    border-radius: 2px;
    min-height: 0.6rem;
  }
  .month-count { font-family: var(--font-mono); color: var(--color-primary); font-weight: 700; }

  .recent-list { display: grid; gap: var(--space-md); }
  .recent-header { display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: var(--space-sm); }
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
