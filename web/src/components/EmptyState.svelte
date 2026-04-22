<script lang="ts">
  interface Props {
    title: string;
    message: string;
    actionHref?: string;
    actionLabel?: string;
  }

  let { title, message, actionHref, actionLabel }: Props = $props();
</script>

<div class="card empty-state" role="status" aria-live="polite">
  <h3>{title}</h3>
  <p>{message}</p>
  {#if actionHref && actionLabel}
    <div class="action">
      <a href={actionHref} class="btn btn-primary">{actionLabel}</a>
    </div>
  {/if}
</div>

<style>
  .action {
    margin-top: var(--space-sm);
    display: flex;
    justify-content: center;
  }
</style>
