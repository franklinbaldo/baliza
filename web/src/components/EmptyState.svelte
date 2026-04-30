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
  .empty-state {
    border-radius: var(--radius-lg) var(--radius-lg) var(--radius-lg) var(--radius-arch);
    border-color: var(--color-border);
    background:
      linear-gradient(color-mix(in srgb, var(--color-bg) 88%, transparent), color-mix(in srgb, var(--color-bg) 88%, transparent)),
      radial-gradient(circle at 0 0, color-mix(in srgb, var(--color-azul) 20%, transparent) 0 5rem, transparent 5.2rem),
      var(--color-surface);
    box-shadow: var(--shadow-pool-sm);
  }
  .empty-state::before {
    content: "";
    width: min(14rem, 70%);
    height: 0.5rem;
    margin-bottom: var(--space-4);
    border-radius: var(--radius-pill);
    background: repeating-linear-gradient(90deg, var(--color-azul) 0 24px, var(--color-ouro) 24px 36px, var(--color-accent) 36px 60px);
  }
  .action {
    margin-top: var(--space-sm);
    display: flex;
    justify-content: center;
  }
</style>
