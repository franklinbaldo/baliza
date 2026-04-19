<script lang="ts">
  import StatCard from './StatCard.svelte';
  import AlertBanner from './AlertBanner.svelte';
  import { SyncStatsSchema, type SyncStats } from '../schema';
  import { formatInteger, formatRelativeTime } from '../lib/format';
  import { onMount } from 'svelte';

  let stats = $state<SyncStats | null>(null);
  let error = $state<string | null>(null);
  let loading = $state(true);

  async function loadStats() {
    loading = true;
    error = null;
    try {
      const res = await fetch('/baliza/data/sync_stats.json');
      if (!res.ok) throw new Error('Falha ao obter os dados pré-compilados pelo DuckDB.');
      const raw = await res.json();
      stats = SyncStatsSchema.parse(raw);
    } catch (e) {
      error = (e as Error).message;
    } finally {
      loading = false;
    }
  }

  onMount(loadStats);
</script>

{#if error}
  <AlertBanner title="Erro de Leitura" message={error} level="error" />
  <div class="retry-row">
    <button class="btn btn-outline" onclick={loadStats}>Tentar novamente</button>
  </div>
{:else if loading}
  <div class="dashboard-grid" aria-busy="true" aria-label="Carregando estatísticas">
    {#each [1, 2, 3] as _, i (i)}
      <div class="card skeleton-card">
        <div class="skeleton skeleton-label"></div>
        <div class="skeleton skeleton-value"></div>
      </div>
    {/each}
  </div>
{:else if stats}
  <div class="dashboard-grid">
    <StatCard
      title="Contratos arquivados"
      value={formatInteger(stats.total_contracts)}
    />
    <StatCard
      title="Dias de histórico preservados"
      value={formatInteger(stats.days_on_ia)}
      hint={stats.generated_at ? `Atualizado ${formatRelativeTime(stats.generated_at)}` : undefined}
    />
    <StatCard
      title="Em quarentena"
      value={formatInteger(stats.total_quarantine)}
      tone="warning"
      hint="O que é isso?"
      href="/baliza/sobre#quarentena"
    />
  </div>
{/if}

<style>
  .dashboard-grid {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-md);
    margin-top: var(--space-md);
  }

  .skeleton-card {
    flex: 1;
    min-width: 160px;
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
    padding: var(--space-md);
  }

  .skeleton-label {
    height: 0.75rem;
    width: 60%;
  }

  .skeleton-value {
    height: 2.5rem;
    width: 80%;
  }

  .retry-row {
    margin-top: var(--space-sm);
    display: flex;
    justify-content: center;
  }
</style>
