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

{#snippet toneIcon()}
  {#if tone === 'success'}
    <svg data-icon aria-hidden="true" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"></circle><path d="m8.5 12 2.5 2.5L15.5 10"></path></svg>
  {:else if tone === 'warning'}
    <svg data-icon aria-hidden="true" viewBox="0 0 24 24"><path d="M12 3 2.5 20h19L12 3Z"></path><path d="M12 10v4"></path><circle cx="12" cy="17" r="0.5" fill="currentColor"></circle></svg>
  {:else if tone === 'danger'}
    <svg data-icon aria-hidden="true" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"></circle><path d="m9 9 6 6M15 9l-6 6"></path></svg>
  {:else}
    <svg data-icon aria-hidden="true" viewBox="0 0 24 24"><rect x="4" y="4" width="16" height="16" rx="2"></rect><path d="M8 12h8M12 8v8"></path></svg>
  {/if}
{/snippet}

{#if href}
  <article>
    <a {href} role="button" class="contrast outline" style="--pico-primary:{accent}">
      <header>
        {@render toneIcon()}
        <h3>{title}</h3>
      </header>
      <p><strong>{value}</strong></p>
      {#if hint}<small>{hint}</small>{/if}
    </a>
  </article>
{:else}
  <article style="--pico-primary:{accent}">
    <header>
      {@render toneIcon()}
      <h3>{title}</h3>
    </header>
    <p><strong>{value}</strong></p>
    {#if hint}<small>{hint}</small>{/if}
  </article>
{/if}
<!-- WHY inline style: only the per-instance accent CSS variable
     varies; all box/typography styling comes from Pico defaults. -->


