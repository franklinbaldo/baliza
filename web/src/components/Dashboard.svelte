<script lang="ts">
  import StatCard from './StatCard.svelte';
  import AlertBanner from './AlertBanner.svelte';
  import Skeleton from './Skeleton.svelte';
  import { SyncStatsSchema, type SyncStats, type ResourceStats } from '../schema';
  import { formatInteger, formatRelativeTime } from '../lib/format';
  import { resolve } from '../lib/baseUrl';
  import { onMount, untrack } from 'svelte';

  let { initialStats }: { initialStats?: SyncStats } = $props();

  // `initialStats` is an SSR seed, not a live prop contract: after hydration,
  // the public JSON endpoint becomes authoritative. Mark the intentional
  // one-time snapshot explicitly so Svelte does not report it as accidental
  // stale-prop capture.
  let stats = $state<SyncStats | null>(untrack(() => initialStats ?? null));
  let error = $state<string | null>(null);
  let loading = $state(untrack(() => !initialStats));

  async function loadStats() {
    error = null;
    if (!stats) loading = true;
    try {
      const res = await fetch(resolve('data/sync_stats.json'));
      if (!res.ok) throw new Error('Falha ao obter os dados pré-compilados pelo DuckDB.');
      const raw = await res.json();
      stats = SyncStatsSchema.parse(raw);
    } catch (e) {
      if (!stats) error = (e as Error).message;
    } finally {
      loading = false;
    }
  }

  onMount(loadStats);

  // Temporal spine order: intent → open → reusable → final.
  const RESOURCE_ORDER = ['pca', 'publicacoes', 'atas', 'contratos'] as const;

  const RESOURCE_META: Record<string, { label: string; hint: string; href: string }> = {
    pca: {
      label: 'PCA',
      hint: 'planos anuais de contratação arquivados',
      href: resolve('pca'),
    },
    publicacoes: {
      label: 'Publicações',
      hint: 'oportunidades em aberto arquivadas',
      href: resolve('publicacoes'),
    },
    atas: {
      label: 'Atas',
      hint: 'atas de registro de preços arquivadas',
      href: resolve('atas'),
    },
    contratos: {
      label: 'Contratos',
      hint: 'contratos citáveis com permalink arquivado',
      href: resolve('busca'),
    },
  };

  function buildTiles(resources: Record<string, ResourceStats> | undefined) {
    if (!resources) return null;
    return RESOURCE_ORDER.map((k) => {
      const meta = RESOURCE_META[k]!;
      const r = resources[k];
      const hasData = r != null && r.row_count > 0;
      return {
        key: k,
        label: meta.label,
        value: hasData ? formatInteger(r.row_count) : 'Sem dados',
        hint: hasData
          ? `${r.partition_count} partições · ${meta.hint}`
          : `${meta.hint} — aguardando primeira sincronização`,
        href: meta.href,
        tone: hasData ? ('default' as const) : ('warning' as const),
      };
    });
  }

  const tiles = $derived(buildTiles(stats?.resources));
</script>

{#if error}
  <AlertBanner title="Erro de Leitura" message={error} level="error" />
  <div class="actions">
    <button class="outline" onclick={loadStats}>Tentar novamente</button>
  </div>
{:else if loading}
  <div class="grid" aria-busy="true" aria-label="Carregando estatísticas">
    {#each [1, 2, 3, 4] as _, i (i)}
      <Skeleton />
    {/each}
  </div>
{:else if stats}
  {#if tiles}
    <div class="grid">
      {#each tiles as tile (tile.key)}
        <StatCard
          title={tile.label}
          value={tile.value}
          hint={tile.hint}
          tone={tile.tone}
          href={tile.href}
        />
      {/each}
    </div>
  {:else}
    <div class="grid">
      <StatCard
        title="Contratos citáveis"
        value={formatInteger(stats.total_contracts)}
        hint="cada um com permalink e snapshot arquivado"
      />
      <StatCard
        title="Dias no Internet Archive"
        value={formatInteger(stats.days_on_ia)}
        hint={stats.generated_at ? `Atualizado ${formatRelativeTime(stats.generated_at)}` : 'fora do alcance de qualquer servidor único'}
      />
      <StatCard
        title="Em quarentena"
        value={formatInteger(stats.total_quarantine)}
        tone="warning"
        hint="anomalias sinalizadas — entenda o critério"
        href={resolve('sobre#quarentena')}
      />
    </div>
  {/if}
  {#if stats.generated_at}
    <p class="update-hint">
      Atualizado {formatRelativeTime(stats.generated_at)} ·
      <a href={resolve('status')}>status do arquivo</a> ·
      <a href={resolve('arquivo')}>coleção no Internet Archive</a>
    </p>
  {/if}
{/if}

<style>
  .update-hint {
    font-size: var(--text-sm);
    color: var(--color-text-dim);
    margin-top: var(--space-3);
    text-align: center;
  }
</style>
