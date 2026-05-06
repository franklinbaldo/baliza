<script lang="ts">
  // Loading placeholder built on Pico's aria-busy=true plus the
  // .is-skeleton hook in global.css. Centralized so every loading
  // state has the same shape and a11y treatment.
  let {
    label = 'Carregando',
    rows = 2,
    messages,
  }: {
    label?: string;
    rows?: number;
    messages?: string[];
  } = $props();

  const NBSP = ' ';
  const lines = $derived(messages ?? Array.from({ length: rows }, () => NBSP));
</script>

<article aria-busy="true" class="is-skeleton" aria-label={label}>
  {#each lines as line, i (i)}<p>{line}</p>{/each}
</article>
