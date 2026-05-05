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
  <button data-testid="glossary-term" type="button" aria-describedby={tooltipId}>
    {#if children}{@render children()}{:else}{term}{/if}
    <span id={tooltipId} role="tooltip">{entry.plain}</span>
  </button>
{:else if children}
  {@render children()}
{:else}
  {term}
{/if}

