<script lang="ts">
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { getQueryClient } from '../lib/queryClient';
  import { QUERY_KEYS } from '../lib/queryKeys';
  import { fetchPublicacaoPagesForObjeto } from '../lib/pncpPublicacao';

  import AlertBanner from './AlertBanner.svelte';
  import EmptyState from './EmptyState.svelte';
  import HubHeader from './HubHeader.svelte';
  import ShareButton from './ShareButton.svelte';
  import Skeleton from './Skeleton.svelte';
  import { replaceUrlParams } from '../lib/urlState';

  setQueryClientContext(getQueryClient());

  const { objeto: objetoProp = '' }: { objeto?: string } = $props();

  function initialObjeto(): string {
    if (objetoProp) return objetoProp;
    if (typeof window === 'undefined') return '';
    return new URLSearchParams(window.location.search).get('objeto') ?? '';
  }

  let searchInput = $state(initialObjeto());
  let submittedObjeto = $state(initialObjeto());

  const query = createQuery(() => {
    const term = submittedObjeto;
    return {
      queryKey: QUERY_KEYS.busca(term),
      enabled: term.length >= 3,
      queryFn: () => fetchPublicacaoPagesForObjeto(term),
    };
  });

  const contracts = $derived(query.data ?? []);
  const loading = $derived(query.isFetching);
  const error = $derived(query.error as Error | null);

  const topBuyers = $derived.by(() => {
    const counts: Record<string, { razaoSocial: string; count: number }> = {};
    for (const c of contracts) {
      if (!c.orgaoEntidade?.cnpj) continue;
      const cnpj = c.orgaoEntidade.cnpj;
      if (!counts[cnpj]) counts[cnpj] = { razaoSocial: c.orgaoEntidade.razaoSocial || cnpj, count: 0 };
      counts[cnpj].count++;
    }
    return Object.values(counts).sort((a, b) => b.count - a.count).slice(0, 5);
  });

  const topSuppliers = $derived.by(() => {
    const counts: Record<string, { name: string; count: number }> = {};
    for (const c of contracts) {
      if (!c.nomeRazaoSocialFornecedor && !c.niFornecedor) continue;
      const id = (c.niFornecedor || c.nomeRazaoSocialFornecedor || 'Unknown') as string;
      const name = (c.nomeRazaoSocialFornecedor || c.niFornecedor || 'Unknown') as string;
      if (!counts[id]) counts[id] = { name, count: 0 };
      counts[id].count++;
    }
    return Object.values(counts).sort((a, b) => b.count - a.count).slice(0, 5);
  });

  const priceRange = $derived.by(() => {
    let min = Infinity;
    let max = -Infinity;
    for (const c of contracts) {
      if (typeof c.valorGlobal === 'number') {
        min = Math.min(min, c.valorGlobal);
        max = Math.max(max, c.valorGlobal);
      } else if (typeof c.valorTotalEstimado === 'number') {
         min = Math.min(min, c.valorTotalEstimado);
         max = Math.max(max, c.valorTotalEstimado);
      }
    }
    return { min: min === Infinity ? null : min, max: max === -Infinity ? null : max };
  });

  const formatBrl = (val: number) =>
    new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(val);

  function handleSubmit(ev: Event) {
    ev.preventDefault();
    const term = searchInput.trim();
    submittedObjeto = term;
    replaceUrlParams({ objeto: term });
  }

  let showPriceRef = $state(false);

  const snapshotDate = new Date().toISOString().slice(0, 10);

  const contractIds = $derived(
    contracts.slice(0, 5).map((c) => c.numeroControlePNCP).filter(Boolean),
  );

  const priceStats = $derived.by(() => {
    const values: number[] = [];
    for (const c of contracts) {
      const v = typeof c.valorGlobal === 'number' && c.valorGlobal > 0
        ? c.valorGlobal
        : typeof c.valorTotalEstimado === 'number' && c.valorTotalEstimado > 0
          ? c.valorTotalEstimado
          : null;
      if (v !== null) values.push(v);
    }
    if (values.length === 0) return null;
    const sorted = [...values].sort((a, b) => a - b);
    const min = sorted[0];
    const max = sorted[sorted.length - 1];
    const avg = values.reduce((s, v) => s + v, 0) / values.length;
    const mid = Math.floor(sorted.length / 2);
    const median = sorted.length % 2 === 0
      ? (sorted[mid - 1] + sorted[mid]) / 2
      : sorted[mid];
    const variance = values.reduce((s, v) => s + (v - avg) ** 2, 0) / values.length;
    const stddev = Math.sqrt(variance);
    return { min, max, avg, median, stddev };
  });
</script>

<section>
  <HubHeader kicker="📊 Mercado" title="Análise de Mercado">
    {#snippet lede()}
      <p>Top compradores, fornecedores e faixa de preço. Fonte: API PNCP.</p>
    {/snippet}
  </HubHeader>

  <form role="search" onsubmit={handleSubmit}>
    <label for="mercado-objeto-input" class="sr-only">Objeto a pesquisar</label>
    <div role="group" aria-label="Buscar mercado">
      <input id="mercado-objeto-input" type="search" bind:value={searchInput} placeholder="Ex.: merenda escolar..." aria-label="Objeto a pesquisar" />
      <button type="submit">Buscar</button>
    </div>
  </form>

  {#if !submittedObjeto || submittedObjeto.length < 3}
    <EmptyState title="Digite o objeto a pesquisar" message="Use ao menos 3 caracteres para ver o panorama do mercado." />
  {:else if loading}
    <Skeleton label="Buscando mercado" />
  {:else if error}
    <AlertBanner title="Não foi possível buscar mercado" message={error.message} level="error" />
  {:else if contracts.length === 0}
    <EmptyState title="Nenhum dado de mercado encontrado" message="Nenhum contrato recente do PNCP correspondeu ao objeto pesquisado." />
  {:else}
    <div class="grid">
      <article data-testid="mercado-count">
        <small>Total de contratos</small>
        <p><strong>{contracts.length}</strong></p>
      </article>
      <article data-testid="mercado-price-range">
        <small>Faixa de preço</small>
        <p><strong>
          {#if priceRange.min !== null && priceRange.max !== null}
            {formatBrl(priceRange.min)} — {formatBrl(priceRange.max)}
          {:else}
            N/D
          {/if}
        </strong></p>
      </article>
    </div>
    <div class="grid">
      <section data-testid="mercado-top-buyers">
        <h2>Top Compradores</h2>
        <ul>
          {#each topBuyers as buyer (buyer.razaoSocial)}<li>{buyer.razaoSocial} <small>({buyer.count})</small></li>{/each}
        </ul>
      </section>
      <section data-testid="mercado-top-suppliers">
        <h2>Top Fornecedores</h2>
        <ul>
          {#each topSuppliers as supplier (supplier.name)}<li>{supplier.name} <small>({supplier.count})</small></li>{/each}
        </ul>
      </section>
    </div>
    <div class="actions">
      <button data-testid="mercado-gerar-pesquisa" onclick={() => (showPriceRef = true)}>Gerar pesquisa de preços</button>
      <ShareButton title={`Mercado: ${submittedObjeto}`} variant="inline" />
    </div>
    {#if showPriceRef && priceStats}
      <section data-testid="mercado-price-ref">
        <h2>Pesquisa de Preços</h2>
        <table>
          <thead>
            <tr>
              <th>Estatística</th>
              <th>Valor</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>Mínimo</td><td data-testid="price-ref-min">{formatBrl(priceStats.min)}</td></tr>
            <tr><td>Média</td><td data-testid="price-ref-avg">{formatBrl(priceStats.avg)}</td></tr>
            <tr><td>Mediana</td><td data-testid="price-ref-median">{formatBrl(priceStats.median)}</td></tr>
            <tr><td>Máximo</td><td data-testid="price-ref-max">{formatBrl(priceStats.max)}</td></tr>
            <tr><td>Desvio Padrão</td><td data-testid="price-ref-stddev">{formatBrl(priceStats.stddev)}</td></tr>
          </tbody>
        </table>
        <p>Data de referência: <span data-testid="price-ref-date">{snapshotDate}</span></p>
        <ul data-testid="price-ref-ids">
          {#each contractIds as id (id)}<li>{id}</li>{/each}
        </ul>
        <button onclick={() => window.print()}>Imprimir / Salvar como PDF</button>
      </section>
    {/if}
  {/if}
</section>

