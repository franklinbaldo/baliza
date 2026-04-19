<script lang="ts">
  type Tone = 'default' | 'success' | 'warning' | 'danger';

  let {
    title,
    value,
    tone = 'default',
    hint,
    href,
  }: {
    title: string;
    value: number | string;
    tone?: Tone;
    hint?: string;
    href?: string;
  } = $props();

  const accentVar: Record<Tone, string> = {
    default: 'var(--color-primary)',
    success: 'var(--color-success)',
    warning: 'var(--color-warning)',
    danger:  'var(--color-error)',
  };
  const accent = $derived(accentVar[tone]);
</script>

{#if href}
  <a class="card card-link" style="--accent: {accent}" {href}>
    <h3 class="stat-label">{title}</h3>
    <p class="stat-value">{value}</p>
    {#if hint}<p class="stat-hint">{hint}</p>{/if}
  </a>
{:else}
  <div class="card" style="--accent: {accent}">
    <h3 class="stat-label">{title}</h3>
    <p class="stat-value">{value}</p>
    {#if hint}<p class="stat-hint">{hint}</p>{/if}
  </div>
{/if}

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
  .card-link {
    display: block;
    color: inherit;
    text-decoration: none;
  }
  .card-link:hover {
    border-color: var(--accent);
    box-shadow: var(--shadow-md);
    transform: translate(-2px, -2px);
  }
  .stat-value {
    color: var(--accent);
    text-shadow: none;
  }
  .stat-hint {
    margin-top: var(--space-xs);
    font-size: var(--font-size-xs);
    color: var(--color-secondary);
    font-family: var(--font-mono);
  }
</style>
