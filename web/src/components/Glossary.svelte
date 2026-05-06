<script lang="ts">
  import { lookupGlossary } from '../lib/glossary';

  interface Props {
    term: string | null | undefined;
    children?: import('svelte').Snippet;
  }

  let { term, children }: Props = $props();

  const entry = $derived(lookupGlossary(term));
  const tooltipId = $derived.by(() => {
    const raw = (term ?? entry?.term ?? 'term').toString();
    const normalized = raw
      .toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/(^-|-$)/g, '');

    return `glossary-tooltip-${normalized || 'term'}`;
  });
</script>

{#if entry}
  <span
    data-testid="glossary-term"
    data-tooltip={entry.plain}
    data-placement="top"
    aria-describedby={tooltipId}
    role="button"
    tabindex="0"
  >
    {#if children}{@render children()}{:else}{term}{/if}
  </span>
{:else if children}
  {@render children()}
{:else}
  {term}
{/if}

