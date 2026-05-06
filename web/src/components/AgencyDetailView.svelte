
<script lang="ts">
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { getQueryClient } from '../lib/queryClient';
  import { QUERY_KEYS } from '../lib/queryKeys';
  import { prefetchArchive } from '../lib/parquetFallback';
  import { archivedContratoToInternalContract, type PNCPContract } from '../lib/pncp';
  import { fetchPublicacaoList } from '../lib/pncpPublicacao';
  import { formatParticao } from '../lib/format';
  import { resolve } from '../lib/baseUrl';
  import { createListQuery } from '../lib/createListQuery';
  import type { ArchivedContrato } from '../lib/archive/schema';
  import EntityDetailLayout from './EntityDetailLayout.svelte';
  import EmptyState from './EmptyState.svelte';
  import LookbackWindow from './LookbackWindow.svelte';
  import ContractCard from './ContractCard.svelte';
  import PaginatedList from './PaginatedList.svelte';
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
        const contracts = rows.map((row) => archivedContratoToInternalContract(row));
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

<EntityDetailLayout
  id={cnpj}
  idValid={cnpjValid}
  entityType="órgão"
  idFormatError="CNPJ fora do formato esperado: 14 dígitos numéricos, sem pontos, traços ou barras."
  {loading}
  {error}
  dataReady={!!data}
  archivedParticao={data?.archived?.dataParticao}
  archiveMessage={`PNCP indisponível — exibindo dados arquivados (última consolidação: ${data?.archived?.dataParticao ? formatParticao(data.archived.dataParticao) : ''}).`}
  kicker="🏛️ ÓRGÃO / ENTIDADE"
  title={data?.name || ""}
>
  {#snippet metaRow()}
    {#if data}
      <small>CNPJ: <code>{data.cnpj}</code></small>
      <small>Fonte: <mark>{data.archived ? 'Arquivo Parquet (IA)' : 'PNCP V1'}</mark></small>
    {/if}
  {/snippet}

  {#snippet headerActions()}
    {#if data}
      <button
        class="outline"
        onclick={() => addWatch('agency', data.cnpj, data.name)}
        disabled={isAgencyWatched}
        aria-pressed={isAgencyWatched}
      >
        {isAgencyWatched ? '✓ Acompanhando' : 'Receber alertas deste órgão'}
      </button>
    {/if}
  {/snippet}

  {#if data}
    {#if data.contracts.length === 0}
      <EmptyState
        title="Nenhuma contratação recente"
        message="O PNCP não retornou contratações recentes para este CNPJ."
        actionHref={resolve('')}
        actionLabel="Voltar à busca"
      />
    {:else}
      <section class="grid">
        <article data-testid="top-suppliers">
          <h3>Top 5 fornecedores</h3>
          {#if data.topSuppliers.length === 0}
            <p><small>
              Dados de fornecedor disponíveis apenas no snapshot Parquet.
              Alterne a janela para consultar o arquivo.
            </small></p>
          {:else}
            <ol>
              {#each data.topSuppliers as s, i (`${s.cnpj || s.name}-${i}`)}
                <li>
                  <small>#{i + 1}</small>
                  <strong>{s.name}</strong>
                  <small>{s.count}</small>
                </li>
              {/each}
            </ol>
          {/if}
        </article>

        <figure data-testid="monthly-chart">
          <figcaption>Contratos por mês (12 meses)</figcaption>
          <ul aria-label="Contratos por mês">
            {#each data.monthly as m (m.month)}
              <li>
                <small>{m.label}</small>
                <progress value={m.count} max={maxMonthly} aria-hidden="true"></progress>
                <small>{m.count}</small>
              </li>
            {/each}
          </ul>
        </figure>
      </section>

      <section>
        <header>
          <h3>Portfólio de Contratações Recentes</h3>
          <LookbackWindow value={dias} onchange={updateDias} />
        </header>
        <PaginatedList items={data.contracts} pageSize={10} resetTrigger={dias}>
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

