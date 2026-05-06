<script lang="ts">
  import CopyButton from './CopyButton.svelte';

  interface Props {
    title: string;
    message: string;
    actionHref?: string;
    actionLabel?: string;
    copyable?: boolean;
  }

  let { title, message, actionHref, actionLabel, copyable = false }: Props = $props();
</script>

<article role="status" aria-live="polite">
  <header><h3><span aria-hidden="true">🪹</span> {title}</h3></header>
  <p>{message}</p>
  {#if (actionHref && actionLabel) || copyable}
    <footer>
      {#if actionHref && actionLabel}
        <a href={actionHref} role="button">{actionLabel}</a>
      {/if}
      {#if copyable}
        <CopyButton text={`${title}: ${message}`} label="Copiar mensagem" />
      {/if}
    </footer>
  {/if}
</article>

