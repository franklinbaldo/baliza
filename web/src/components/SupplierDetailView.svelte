<script lang="ts">
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { getQueryClient } from '../lib/queryClient';
  import { QUERY_KEYS } from '../lib/queryKeys';
  import {
    archiveErrorMessage,
    prefetchArchive,
    queryArchivedTable,
    queryParquetFallback,
  } from '../lib/parquetFallback';
  import { archivedContratoToInternalContract, type PNCPContract } from '../lib/pncp';
  import { formatBRL, formatParticao } from '../lib/format';
  import { resolve } from '../lib/baseUrl';
  import type { ArchivedContrato } from '../lib/archive/schema';
  import EntityDetailLayout from './EntityDetailLayout.svelte';
  import EmptyState from './EmptyState.svelte';
  import ContractCard from './ContractCard.svelte';

  setQueryClientContext(getQueryClient());

  const { cnpj: cnpjProp = '' }: { cnpj?: string } = $props();

  const cnpj = $derived(
    cnpjProp ||
      (typeof window !== 'undefined'
        ? new URLSearchParams(window.location.search).get('cnpj') ?? ''
        : ''),
  );

  const cnpjValid = $derived(/^\d{14}$/.test(cnpj));

  $effect(() => {
    if (cnpj) prefetchArchive('contratos');
  });

  interface CompetitorTally {
    cnpj: string;
    name: string;
    count: number;
  }

  interface SupplierView {
    name: string;
    cnpj: string;
    contracts: PNCPContract[];
    avgTicket: number;
    topBuyerCnpj: string;
    archived: { dataParticao: string | null };
  }

  function computeAvgTicket(rows: ArchivedContrato[]): number {
    const vals = rows
      .map((r) => r.valor_global ?? r.valor_inicial ?? 0)
      .filter((v): v is number => typeof v === 'number' && v > 0);
    if (vals.length === 0) return 0;
    return vals.reduce((sum, v) => sum + v, 0) / vals.length;
  }

  // "Top buyer" = the órgão this supplier sells to most often in the snapshot.
  // Used to seed the peer-competitor query: we list that órgão's contracts and
  // tally suppliers other than this one. It's a rough proxy for "who competes
  // for this supplier's bread-and-butter buyer", not a market-wide ranking.
  function topBuyer(rows: ArchivedContrato[]): string {
    // eslint-disable-next-line svelte/prefer-svelte-reactivity
    const tally = new Map<string, number>();
    for (const r of rows) {
      const k = r.cnpj_orgao ?? '';
      if (!k) continue;
      tally.set(k, (tally.get(k) ?? 0) + 1);
    }
    let bestKey = '';
    let bestCount = 0;
    for (const [k, c] of tally) {
      if (c > bestCount) {
        bestKey = k;
        bestCount = c;
      }
    }
    return bestKey;
  }

  function computeCompetitors(
    rows: ArchivedContrato[],
    selfCnpj: string,
  ): CompetitorTally[] {
    // eslint-disable-next-line svelte/prefer-svelte-reactivity
    const tally = new Map<string, CompetitorTally>();
    for (const r of rows) {
      const c = r.ni_fornecedor ?? '';
      if (!c || c === selfCnpj) continue;
      const existing = tally.get(c);
      if (existing) {
        existing.count += 1;
      } else {
        tally.set(c, {
          cnpj: c,
          name: r.nome_razao_social_fornecedor ?? c,
          count: 1,
        });
      }
    }
    return [...tally.values()].sort((a, b) => b.count - a.count).slice(0, 3);
  }

  // PNCP has no supplier-by-CNPJ endpoint, so we skip createListQuery and hit
  // the archive directly. createListQuery treats archive `empty` as an error,
  // but for a valid-CNPJ-with-no-history search that should be an empty-state
  // (the supplier exists, they just have no contracts in the snapshot), not
  // an AlertBanner. We handle the three archive outcomes explicitly here.
  const supplierQuery = createQuery(() => ({
    queryKey: QUERY_KEYS.fornecedor(cnpj),
    enabled: !!cnpj,
    queryFn: async (): Promise<SupplierView> => {
      const archived = await queryParquetFallback<ArchivedContrato>(
        'ni_fornecedor',
        cnpj,
        50,
        'data_publicacao_pncp',
      );
      if (archived.ok) {
        return {
          name: archived.rows[0]?.nome_razao_social_fornecedor ?? cnpj,
          cnpj,
          contracts: archived.rows.map((row) => archivedContratoToInternalContract(row)),
          avgTicket: computeAvgTicket(archived.rows),
          topBuyerCnpj: topBuyer(archived.rows),
          archived: { dataParticao: archived.dataParticao },
        };
      }
      if (archived.reason === 'empty') {
        // Valid CNPJ, no contracts — render the empty state against a
        // well-formed view model rather than an error.
        return {
          name: cnpj,
          cnpj,
          contracts: [],
          avgTicket: 0,
          topBuyerCnpj: '',
          archived: { dataParticao: null },
        };
      }
      throw new Error(archiveErrorMessage(archived.reason));
    },
  }));

  const data = $derived(supplierQuery.data);
  const loading = $derived(supplierQuery.isFetching);
  const error = $derived(supplierQuery.error as Error | null);
  const topBuyerCnpj = $derived(data?.topBuyerCnpj ?? '');

  const peerQuery = createQuery(() => ({
    queryKey: ['peer-suppliers', cnpj, topBuyerCnpj] as const,
    enabled: !!cnpj && !!topBuyerCnpj,
    queryFn: async (): Promise<CompetitorTally[]> => {
      // orderByColumn pins the 100-row window to the most recent contracts;
      // without it DuckDB can return an arbitrary subset and the competitor
      // tally fluctuates between loads for larger buyers.
      const r = await queryArchivedTable('contratos', 'cnpj_orgao', topBuyerCnpj, {
        limit: 100,
        orderByColumn: 'data_publicacao_pncp',
      });
      return r.ok ? computeCompetitors(r.rows, cnpj) : [];
    },
    staleTime: Infinity,
  }));

  const competitors = $derived(peerQuery.data ?? []);
</script>

<EntityDetailLayout
  id={cnpj}
  idValid={cnpjValid}
  entityType="fornecedor"
  idFormatError="CNPJ fora do formato esperado: 14 dígitos numéricos, sem pontos, traços ou barras."
  {loading}
  {error}
  dataReady={!!data}
  archivedParticao={data?.archived?.dataParticao}
  archiveMessage={`PNCP não expõe histórico por fornecedor — exibindo snapshot Parquet (última consolidação: ${data?.archived?.dataParticao ? formatParticao(data.archived.dataParticao) : ''}).`}
  kicker="🏢 FORNECEDOR"
  iconId="t2"
  title={data?.name || ""}
>
  {#snippet metaRow()}
    {#if data}
      <span>CNPJ: {data.cnpj}</span>
      <span>Fonte: Arquivo Parquet (IA)</span>
    {/if}
  {/snippet}

  {#if data}
    {#if data.contracts.length === 0}
      <EmptyState
        title="Nenhum contrato arquivado"
        message="O arquivo consolidado não registra contratos para este CNPJ de fornecedor."
        actionHref={resolve('')}
        actionLabel="Voltar à busca"
      />
    {:else}
      <section class="rollup-grid">
        <article class="rollup-card" data-testid="avg-ticket">
          <h3>Ticket médio</h3>
          <p class="big-figure">{formatBRL(data.avgTicket)}</p>
          <p class="muted">Média dos valores globais dos últimos 50 contratos no arquivo.</p>
        </article>

        <article class="rollup-card" data-testid="competing-suppliers">
          <h3>Fornecedores concorrentes</h3>
          {#if competitors.length === 0}
            <p class="muted">
              Sem dados suficientes para listar concorrentes no órgão com mais contratações deste fornecedor.
            </p>
          {:else}
            <ol class="competitor-list">
              {#each competitors as c, i (`${c.cnpj}-${i}`)}
                <li>
                  <span class="competitor-rank">#{i + 1}</span>
                  <span class="competitor-name">{c.name}</span>
                  <span class="competitor-cnpj">{c.cnpj}</span>
                  <span class="competitor-count">{c.count}</span>
                </li>
              {/each}
            </ol>
          {/if}
        </article>
      </section>

      <section class="recent-list" data-testid="contract-history">
        <div class="recent-header">
          <h3>Histórico de contratações (últimos 50 do arquivo)</h3>
        </div>
        {#each data.contracts as item (item.numeroControlePNCP)}
          <ContractCard
            id={item.numeroControlePNCP}
            date={item.dataPublicacaoPncp}
            obj={item.objetoContratacao}
            valor={item.valorTotalEstimado}
            buyer={item.orgaoEntidade.razaoSocial}
          />
        {/each}
      </section>
    {/if}
  {/if}
</EntityDetailLayout>

<style>

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

  .big-figure {
    margin: 0 0 var(--space-xs);
    font-family: var(--font-mono);
    font-size: var(--font-size-xl);
    font-weight: 800;
    color: var(--color-primary);
  }

  .competitor-list { list-style: none; padding: 0; margin: 0; display: grid; gap: var(--space-xs); }
  .competitor-list li {
    display: grid;
    grid-template-columns: auto 1fr auto auto;
    gap: var(--space-sm);
    align-items: baseline;
    font-size: var(--font-size-sm);
    padding: 4px 0;
    border-bottom: 1px solid var(--color-base-200);
  }
  .competitor-list li:last-child { border-bottom: none; }
  .competitor-rank { font-family: var(--font-mono); font-weight: 700; color: var(--color-secondary); }
  .competitor-name { overflow-wrap: anywhere; }
  .competitor-cnpj { font-family: var(--font-mono); font-size: var(--font-size-xs); color: var(--color-secondary); }
  .competitor-count { font-family: var(--font-mono); color: var(--color-primary); font-weight: 700; }

  .recent-list { display: grid; gap: var(--space-md); }
  .recent-header { display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: var(--space-sm); }
</style>
