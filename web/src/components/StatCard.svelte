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
    <div class="card-title-bar">
      <svg width="20" height="20" aria-hidden="true" style="margin-right: 6px; flex-shrink: 0; fill: currentColor; opacity: 0.7"><use href="#t2"/></svg>
      <h3 class="stat-label">{title}</h3>
    </div>
    <p class="stat-value">{value}</p>
    {#if hint}<p class="stat-hint">{hint}</p>{/if}
  </a>
{:else}
  <div class="card" style="--accent: {accent}">
    <div class="card-title-bar">
      <svg width="20" height="20" aria-hidden="true" style="margin-right: 6px; flex-shrink: 0; fill: currentColor; opacity: 0.7"><use href="#t1"/></svg>
      <h3 class="stat-label">{title}</h3>
    </div>
    <p class="stat-value">{value}</p>
    {#if hint}<p class="stat-hint">{hint}</p>{/if}
  </div>
{/if}

<style>
  .card {
    background: var(--neutral-0);
    border: 1px solid var(--neutral-100);
    border-left: 4px solid var(--accent);
    border-radius: var(--radius-0);
    padding: var(--space-4);
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
  .card-title-bar {
    display: flex;
    align-items: center;
    color: var(--neutral-500);
    margin-bottom: var(--space-2);
  }
  .stat-value {
    color: var(--accent);
    text-shadow: none;
  }
  .stat-hint {
    margin-top: var(--space-2);
    font-size: var(--text-xs);
    color: var(--neutral-500);
    font-family: var(--font-mono);
  }
</style>
