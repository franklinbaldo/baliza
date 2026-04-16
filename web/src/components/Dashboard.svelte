<script lang="ts">
  import StatCard from './StatCard.svelte';
  import { SyncStatsSchema, type SyncStats } from '../schema';
  import { onMount } from 'svelte';

  let stats = $state<SyncStats | null>(null);
  let error = $state<string | null>(null);

  onMount(async () => {
    try {
      // the base path configured in astro.config.mjs is /baliza
      const res = await fetch('/baliza/data/sync_stats.json');
      if (!res.ok) throw new Error('Falha ao obter os dados pré-compilados pelo DuckDB.');
      const raw = await res.json();
      stats = SyncStatsSchema.parse(raw);
    } catch (e) {
      error = (e as Error).message;
    }
  });
</script>

{#if error}
  <div class="alert" role="alert">
    <strong>Erro de Leitura:</strong> {error}
  </div>
{:else if !stats}
  <div class="dashboard-grid">
    <div class="card stat-card animate-shimmer" style="height: 120px;"></div>
    <div class="card stat-card animate-shimmer" style="height: 120px; animation-delay: 0.1s;"></div>
    <div class="card stat-card animate-shimmer" style="height: 120px; animation-delay: 0.2s;"></div>
  </div>
{:else}
  <div class="dashboard-grid animate-fade-in">
    <StatCard title="Contratos Registrados" value={stats.total_contracts} />
    <StatCard title="Dias Preservados" value={stats.days_on_ia} />
    <StatCard title="Total em Quarentena" value={stats.total_quarantine} />
  </div>
{/if}

<style>
  .dashboard-grid {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-md);
    margin-top: var(--space-md);
  }
  .alert {
    background: var(--color-error);
    color: var(--color-base-100);
    padding: var(--space-sm);
    border-radius: var(--radius-sm);
  }
  .stat-card {
    flex: 1;
    min-width: 250px;
  }
</style>
