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
  <section class="wow-strip" aria-label={`Pulso de ${cityState.nome}`}>
    <div class="container">
      <div class="wow-grid">
        {#if loading}
          {#each [1, 2, 3, 4] as _, col (col)}
            <div class="wow-tile wow-tile--loading" aria-busy="true">
              <div class="skeleton skel-num"></div>
              <div class="skeleton skel-label"></div>
            </div>
          {/each}
        {:else if data}
          <a class="wow-tile" href={buscaHref} aria-label={`Ver os ${formatInteger(data.contratos)} contratos arquivados de ${cityState.nome}`}>
            <span class="wow-num">{formatInteger(data.contratos)}</span>
            <span class="wow-label">contratos arquivados</span>
            {#if desdeLabel}<span class="wow-foot">{desdeLabel}</span>{/if}
          </a>

          <a class="wow-tile" href={buscaHref} aria-label={`Ver contratos de ${cityState.nome} (valor total ${formatBRL(data.valorTotal)})`}>
            <span class="wow-num">{formatBRL(data.valorTotal)}</span>
            <span class="wow-label">valor estimado total</span>
          </a>

          {#if data.topOrgao}
            <a class="wow-tile" href={resolve(`orgao?cnpj=${data.topOrgao.cnpj}`)} aria-label={`Ver contratos do órgão ${data.topOrgao.razaoSocial ?? data.topOrgao.cnpj}`}>
              <span class="wow-num wow-num--text">{data.topOrgao.razaoSocial ?? data.topOrgao.cnpj}</span>
              <span class="wow-label">maior comprador</span>
              <span class="wow-foot">{formatInteger(data.topOrgao.n)} contratos</span>
            </a>
          {:else}
            <div class="wow-tile wow-tile--meta" role="status">
              <span class="wow-num wow-num--em">—</span>
              <span class="wow-label">sem comprador frequente</span>
            </div>
          {/if}

          <div class="wow-tile wow-tile--meta" role="status" aria-live="polite">
            <span class="wow-num wow-num--small">{ateLabel || '—'}</span>
            <span class="wow-label">snapshot via Internet Archive</span>
          </div>
        {/if}
      </div>

      {#if data && data.contratos > 0}
        <a class="wow-cta" href={buscaHref}>
          Ver todos os contratos de {cityState.nome} →
        </a>
      {:else if data}
        <p class="wow-empty" aria-live="polite">
          Nenhum contrato arquivado para {cityState.nome} até agora.
        </p>
      {/if}
    </div>
  </section>
{/if}

<style>
  .wow-strip {
    padding: var(--space-6) 0 var(--space-8);
    background:
      linear-gradient(color-mix(in srgb, var(--color-surface) 94%, transparent), color-mix(in srgb, var(--color-surface) 94%, transparent)),
      var(--color-surface);
    border-bottom: 1px solid var(--color-border);
    position: relative;
  }
  .wow-strip::before {
    content: "";
    display: block;
    width: min(14rem, 40vw);
    height: 5px;
    margin: 0 0 var(--space-5);
    background: repeating-linear-gradient(90deg, var(--color-accent) 0 36px, var(--color-ouro) 36px 50px, var(--color-azul) 50px 82px);
  }
  .wow-grid {
    display: grid;
    grid-template-columns: 1fr;
    gap: var(--space-3);
  }
  @media (min-width: 720px) {
    .wow-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  }
  @media (min-width: 1024px) {
    .wow-grid { grid-template-columns: repeat(4, minmax(0, 1fr)); }
  }
  .wow-tile {
    display: grid;
    gap: var(--space-1);
    padding: var(--space-5);
    background: var(--color-bg);
    border: 1px solid var(--color-border);
    border-left: 5px solid var(--color-azul);
    text-decoration: none;
    color: inherit;
    transition: transform var(--duration-fast) var(--ease), box-shadow var(--duration-fast) var(--ease), border-color var(--duration-fast) var(--ease);
  }
  .wow-tile:nth-child(2) { border-left-color: var(--color-verde); }
  .wow-tile:nth-child(3) { border-left-color: var(--color-ouro); }
  .wow-tile:nth-child(4) { border-left-color: var(--color-tijolo); }
  a.wow-tile:hover,
  a.wow-tile:focus-visible {
    transform: translateY(-2px);
    border-color: var(--color-azul);
    box-shadow: var(--shadow-pool-sm);
  }
  .wow-tile--meta {
    background: color-mix(in srgb, var(--color-surface) 60%, transparent);
  }
  .wow-num {
    font-family: var(--font-display);
    font-weight: 300;
    font-size: clamp(28px, 3.5vw, 44px);
    line-height: 1.1;
    color: var(--color-text);
    letter-spacing: -0.01em;
  }
  .wow-num--text {
    font-size: clamp(18px, 2vw, 24px);
    font-weight: 400;
    line-height: 1.25;
    /* Capitals' biggest "buyer" can be a long razão social — clamp to two
       lines so the tile height stays roughly aligned with the numeric ones. */
    display: -webkit-box;
    -webkit-line-clamp: 2;
    line-clamp: 2;
    -webkit-box-orient: vertical;
    overflow: hidden;
  }
  .wow-num--small { font-size: clamp(20px, 2.4vw, 28px); }
  .wow-num--em { color: var(--color-text-dim); }
  .wow-label {
    font-family: var(--font-mono);
    font-size: var(--text-xs);
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: var(--color-text-dim);
  }
  .wow-foot {
    font-size: var(--text-xs);
    color: var(--color-text-dim);
  }
  .wow-cta {
    display: inline-block;
    margin-top: var(--space-5);
    font-family: var(--font-mono);
    font-size: var(--text-sm);
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--color-accent);
    border-bottom: 2px solid transparent;
    padding: 2px 0;
  }
  .wow-cta:hover,
  .wow-cta:focus-visible {
    border-bottom-color: var(--color-accent);
  }
  .wow-empty {
    margin-top: var(--space-5);
    color: var(--color-text-dim);
    font-size: var(--text-sm);
  }
  /* Skeleton tile internals — reuses the global .skeleton shimmer. */
  .skel-num { height: 2.4rem; width: 70%; margin-bottom: var(--space-2); }
  .skel-label { height: 0.9rem; width: 50%; }
</style>
