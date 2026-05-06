<script lang="ts">
  // Top-of-page header used by every "hub" view (Atas, Busca, Comparar,
  // Dispensas, Mercado): eyebrow kicker + h1 title + optional lede body.
  // Centralized so the kicker treatment, hgroup nesting, and a11y
  // semantics stay consistent across views.
  import type { Snippet } from 'svelte';

  let {
    kicker,
    title,
    lede,
  }: {
    kicker: string;
    title: string;
    lede?: Snippet;
  } = $props();
</script>

<header>
  <hgroup>
    <p class="eyebrow">{kicker}</p>
    <h1>{title}</h1>
    {#if lede}{@render lede()}{/if}
  </hgroup>
</header>
