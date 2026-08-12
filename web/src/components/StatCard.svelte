<script lang="ts">
  import Icon from './Icon.svelte';
  import type { IconName } from '../lib/icons';
  type Tone = 'default' | 'success' | 'warning' | 'danger';

  let {
    title,
    value,
    tone = 'default',
    hint,
    href,
    datetime,
  }: {
    title: string;
    value: number | string;
    tone?: Tone;
    hint?: string;
    href?: string;
    datetime?: string;
  } = $props();

  const accentVar: Record<Tone, string> = {
    default: 'var(--color-primary)',
    success: 'var(--color-success)',
    warning: 'var(--color-warning)',
    danger:  'var(--color-error)',
  };
  const toneIconName: Record<Tone, IconName> = {
    default: 'hash',
    success: 'check-circle',
    warning: 'warning',
    danger: 'x-circle',
  };
  const accent = $derived(accentVar[tone]);
  const iconName = $derived(toneIconName[tone]);
</script>

{#if href}
  <article>
    <a {href} role="button" class="contrast outline" style="--pico-primary:{accent}">
      <header>
        <Icon name={iconName} />
        <h3>{title}</h3>
      </header>
      <p><strong>{#if datetime}<time {datetime}>{value}</time>{:else}{value}{/if}</strong></p>
      {#if hint}<small>{hint}</small>{/if}
    </a>
  </article>
{:else}
  <article style="--pico-primary:{accent}">
    <header>
      <Icon name={iconName} />
      <h3>{title}</h3>
    </header>
    <p><strong>{#if datetime}<time {datetime}>{value}</time>{:else}{value}{/if}</strong></p>
    {#if hint}<small>{hint}</small>{/if}
  </article>
{/if}
<!-- WHY inline style: only the per-instance accent CSS variable
     varies; all box/typography styling comes from Pico defaults. -->


