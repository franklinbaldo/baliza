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
  <button class="glossary-wrap" data-testid="glossary-term" type="button" aria-describedby={tooltipId}>
    {#if children}{@render children()}{:else}{term}{/if}
    <span class="glossary-tooltip" id={tooltipId} role="tooltip">{entry.plain}</span>
  </button>
{:else if children}
  {@render children()}
{:else}
  {term}
{/if}

<style>
  .glossary-wrap {
    position: relative;
    display: inline;
    padding: 0;
    margin: 0;
    border: none;
    border-bottom: 1px dotted var(--color-secondary);
    background: none;
    color: inherit;
    font: inherit;
    text-align: inherit;
    cursor: help;
    outline: none;
  }
  .glossary-wrap:focus,
  .glossary-wrap:focus-visible {
    outline: 2px solid var(--color-primary);
    outline-offset: 2px;
  }
  .glossary-tooltip {
    position: absolute;
    top: calc(100% + 4px);
    left: 0;
    z-index: 10;
    width: max-content;
    max-width: 280px;
    padding: 8px 10px;
    background: var(--color-base-100);
    border: 1px solid var(--color-base-300);
    border-radius: var(--radius-sm);
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.12);
    color: var(--color-base-content);
    font-size: var(--font-size-xs);
    line-height: 1.4;
    text-align: left;
    white-space: normal;
    opacity: 0;
    visibility: hidden;
    transition: opacity 0.12s ease, visibility 0.12s ease;
    pointer-events: none;
  }
  .glossary-wrap:hover .glossary-tooltip,
  .glossary-wrap:focus .glossary-tooltip,
  .glossary-wrap:focus-within .glossary-tooltip {
    opacity: 1;
    visibility: visible;
  }
</style>
