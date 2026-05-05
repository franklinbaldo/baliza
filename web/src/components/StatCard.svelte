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
  <a {href}>
    <div>
      <svg width="20" height="20" aria-hidden="true"><use href="#t2"/></svg>
      <h3>{title}</h3>
    </div>
    <p>{value}</p>
    {#if hint}<p>{hint}</p>{/if}
  </a>
{:else}
  <div>
    <div>
      <svg width="20" height="20" aria-hidden="true"><use href="#t1"/></svg>
      <h3>{title}</h3>
    </div>
    <p>{value}</p>
    {#if hint}<p>{hint}</p>{/if}
  </div>
{/if}

