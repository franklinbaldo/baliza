<script lang="ts">
  interface Props {
    title: string;
    message: string;
    actionHref?: string;
    actionLabel?: string;
  }

  let { title, message, actionHref, actionLabel }: Props = $props();
</script>

<div role="status" aria-live="polite">
  <h3>{title}</h3>
  <p>{message}</p>
  {#if actionHref && actionLabel}
    <div>
      <a href={actionHref}>{actionLabel}</a>
    </div>
  {/if}
</div>

