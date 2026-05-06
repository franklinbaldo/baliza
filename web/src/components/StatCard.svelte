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
  <article>
    <a {href} role="button" class="contrast outline" style="--pico-primary:{accent}">
      <header>
        <svg width="20" height="20" aria-hidden="true"><use href="#t2"/></svg>
        <h3>{title}</h3>
      </header>
      <p><strong>{value}</strong></p>
      {#if hint}<small>{hint}</small>{/if}
    </a>
  </article>
{:else}
  <article style="--pico-primary:{accent}">
    <header>
      <svg width="20" height="20" aria-hidden="true"><use href="#t1"/></svg>
      <h3>{title}</h3>
    </header>
    <p><strong>{value}</strong></p>
    {#if hint}<small>{hint}</small>{/if}
  </article>
{/if}
<!-- WHY inline style: only the per-instance accent CSS variable
     varies; all box/typography styling comes from Pico defaults. -->


