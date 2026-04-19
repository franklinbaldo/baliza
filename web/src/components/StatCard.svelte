<script lang="ts">
  type Tone = 'default' | 'success' | 'warning' | 'danger';

  let { title, value, tone = 'default' }: { title: string; value: number | string; tone?: Tone } = $props();

  const accentVar: Record<Tone, string> = {
    default: 'var(--color-primary)',
    success: 'var(--color-success)',
    warning: 'var(--color-warning)',
    danger:  'var(--color-error)',
  };
  const accent = $derived(accentVar[tone]);
</script>

<div class="card" style="--accent: {accent}">
  <h3 class="stat-label">{title}</h3>
  <p class="stat-value">{value}</p>
</div>

<style>
  .card {
    background: var(--color-base-100);
    border: 1px solid var(--color-base-300);
    border-left: 4px solid var(--accent);
    border-radius: var(--radius-box);
    padding: var(--space-md);
    flex: 1;
    min-width: 250px;
    position: relative;
    overflow: hidden;
  }
  .stat-value {
    color: var(--accent);
    text-shadow: none;
  }
</style>
