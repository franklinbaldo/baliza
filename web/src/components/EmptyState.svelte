<script lang="ts">
  interface Props {
    title: string;
    message: string;
    actionHref?: string;
    actionLabel?: string;
  }

  let { title, message, actionHref, actionLabel }: Props = $props();
</script>

<article role="status" aria-live="polite">
  <header><h3><span aria-hidden="true">🪹</span> {title}</h3></header>
  <p>{message}</p>
  {#if actionHref && actionLabel}
    <footer>
      <a href={actionHref} role="button">{actionLabel}</a>
    </footer>
  {/if}
</article>

