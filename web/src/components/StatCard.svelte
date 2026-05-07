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
  const toneEmoji: Record<Tone, string> = {
    default: '🔢',
    success: '',
    warning: '⚠️',
    danger:  '❌',
  };
  const accent = $derived(accentVar[tone]);
  const emoji = $derived(toneEmoji[tone]);
</script>

{#if href}
  <article>
    <a {href} role="button" class="contrast outline" style="--pico-primary:{accent}">
      <header>
        {#if tone === 'success'}
          <svg data-icon aria-hidden="true" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><path d="m8 12 3 3 5-6"/></svg>
        {:else}
          <span aria-hidden="true">{emoji}</span>
        {/if}
        <h3>{title}</h3>
      </header>
      <p><strong>{value}</strong></p>
      {#if hint}<small>{hint}</small>{/if}
    </a>
  </article>
{:else}
  <article style="--pico-primary:{accent}">
    <header>
      <span aria-hidden="true">{emoji}</span>
      <h3>{title}</h3>
    </header>
    <p><strong>{value}</strong></p>
    {#if hint}<small>{hint}</small>{/if}
  </article>
{/if}
<!-- WHY inline style: only the per-instance accent CSS variable
     varies; all box/typography styling comes from Pico defaults. -->


