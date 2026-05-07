<script lang="ts">
  import StatCard from './StatCard.svelte';
  import AlertBanner from './AlertBanner.svelte';
  import Skeleton from './Skeleton.svelte';
  import { SyncStatsSchema, type SyncStats } from '../schema';
  import { formatInteger, formatRelativeTime } from '../lib/format';
  import { resolve } from '../lib/baseUrl';
  import { onMount } from 'svelte';

  const { initialStats }: { initialStats?: SyncStats } = $props();
  const seed = initialStats;

  let stats = $state<SyncStats | null>(seed ?? null);
  let error = $state<string | null>(null);
  // When the page passes pre-rendered stats, the first paint shows real
  // numbers; the background refresh below replaces them once it lands.
  let loading = $state(!seed);

  async function loadStats() {
    if (!stats) loading = true;
    error = null;
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
</script>

{#if error}
  <AlertBanner title="Erro de Leitura" message={error} level="error" />
  <div class="actions">
    <button class="outline" onclick={loadStats}>Tentar novamente</button>
  </div>
{:else if loading}
  <div class="grid" aria-busy="true" aria-label="Carregando estatísticas">
    {#each [1, 2, 3] as _, i (i)}
      <Skeleton />
    {/each}
  </div>
{:else if stats}
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

