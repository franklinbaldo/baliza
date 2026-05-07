<script lang="ts">
  import { createQuery, setQueryClientContext } from '@tanstack/svelte-query';
  import { getQueryClient } from '../lib/queryClient';
  import { QUERY_KEYS } from '../lib/queryKeys';
  import { cityState, hydrateCityContext } from '../lib/cityContext.svelte';
  import { resolve } from '../lib/baseUrl';
  import { formatBRL, formatInteger, formatParticao } from '../lib/format';
  import {
    queryCityAggregates,
    prefetchArchive,
    type CityArchiveAggregates,
  } from '../lib/parquetFallback';
  import Skeleton from './Skeleton.svelte';

  setQueryClientContext(getQueryClient());
  hydrateCityContext();

  // Warm the parquet runtime as soon as the island mounts so the very first
  // visit hits a hot DuckDB instance — the strip is the page's wow moment;
  // making it wait on a 2-3 MB cold WASM download would defeat the point.
  prefetchArchive('contratos');

  const ibge = $derived(cityState.ibge);
  const buscaHref = $derived(resolve(`busca?ibge=${ibge}`));

  const aggregatesQuery = createQuery(() => ({
    queryKey: QUERY_KEYS.cityAggregates(ibge),
    enabled: !!ibge,
    // Archive is rebuilt at most daily — any cached value within 5 minutes
    // is fine, and tanstack-query refetches in the background after that.
    staleTime: 5 * 60 * 1000,
    queryFn: async (): Promise<CityArchiveAggregates> => {
      const r = await queryCityAggregates(ibge);
      if (!r.ok) throw new Error(r.reason);
      return r.rows[0];
    },
  }));

  const data = $derived(aggregatesQuery.data);
  const loading = $derived(!!ibge && aggregatesQuery.isFetching && !data);
  const error = $derived(aggregatesQuery.error as Error | null);

  // First-snapshot label: render only when we know the partition. Avoids
  // showing "desde —" while loading.
  const desdeLabel = $derived(
    data?.primeiraParticao
      ? `desde ${formatParticao(data.primeiraParticao).slice(3)}` // drop the day, keep MM/YYYY
      : '',
  );
  const ateLabel = $derived(
    data?.ultimaParticao ? `até ${formatParticao(data.ultimaParticao).slice(0, 5)}` : '',
  );
</script>

<!--
  CityWowStrip — the homepage's "wow moment". Four big-number tiles
  computed in-browser from the archived parquet via DuckDB-WASM (see
  parquetFallback#queryCityAggregates). Sub-second after the first
  parquet shard is cached. Each tile (except the snapshot meta) links
  to the canonical /busca?ibge=X view so the user can drill into the
  detailed live list. When the archive is unavailable the entire
  section hides silently — never blocks the rest of the page.
-->
{#if !ibge}
  <!-- City context not hydrated yet; let CityHero own the empty state. -->
{:else if error}
  <!-- Hide silently. Reasoning: the strip is decorative magnitude; the
       hero above already invites the user to pick a city, and MonitorGrid
       below handles "what next" prompts that don't depend on the archive. -->
{:else}
  <section aria-label={`Pulso de ${cityState.nome}`}>
    <div class="grid">
      {#if loading}
        {#each ['Calculando contratações…', 'Somando R$ executados…', 'Procurando maior comprador…', 'Lendo Internet Archive…'] as msg (msg)}
          <Skeleton messages={[msg]} />
        {/each}
      {:else if data}
        <article>
          <a href={buscaHref} aria-label={`Ver os ${formatInteger(data.contratos)} contratos arquivados de ${cityState.nome}`}>
            <strong>{formatInteger(data.contratos)}</strong>
            <p><small>contratos arquivados</small></p>
            {#if desdeLabel}<small>{desdeLabel}</small>{/if}
          </a>
        </article>

        <article>
          <a href={buscaHref} aria-label={`Ver contratos de ${cityState.nome} (valor total ${formatBRL(data.valorTotal)})`}>
            <strong>{formatBRL(data.valorTotal)}</strong>
            <p><small>valor estimado total</small></p>
          </a>
        </article>

        {#if data.topOrgao}
          <article>
            <a href={resolve(`orgao?cnpj=${data.topOrgao.cnpj}`)} aria-label={`Ver contratos do órgão ${data.topOrgao.razaoSocial ?? data.topOrgao.cnpj}`}>
              <strong>{data.topOrgao.razaoSocial ?? data.topOrgao.cnpj}</strong>
              <p><small>maior comprador</small></p>
              <small>{formatInteger(data.topOrgao.n)} contratos</small>
            </a>
          </article>
        {:else}
          <article role="status"><strong>—</strong><p><small>sem comprador frequente</small></p></article>
        {/if}

        <article role="status" aria-live="polite">
          <strong>{ateLabel || '—'}</strong>
          <p><small>snapshot via Internet Archive</small></p>
        </article>
      {/if}
    </div>

    {#if data && data.contratos > 0}
      <p><a href={buscaHref}>Ver todos os contratos de {cityState.nome} →</a></p>
    {:else if data}
      <p aria-live="polite"><small>Nenhum contrato arquivado para {cityState.nome} até agora.</small></p>
    {/if}
  </section>
{/if}

