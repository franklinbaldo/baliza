<script lang="ts">
  // Loading placeholder built on Pico's aria-busy=true plus the
  // .is-skeleton hook in global.css. Centralized so every loading
  // state has the same shape and a11y treatment.
  let {
    label = 'Carregando',
    rows = 2,
  }: {
    label?: string;
    rows?: number;
  } = $props();
</script>

<article aria-busy="true" class="is-skeleton" aria-label={label}>
  {#each { length: rows } as _, i (i)}<p>&nbsp;</p>{/each}
</article>
