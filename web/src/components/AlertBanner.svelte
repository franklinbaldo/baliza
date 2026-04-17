<script lang="ts">
  interface Props {
    title: string;
    message: string;
    level?: 'info' | 'success' | 'warning' | 'error';
  }

  let { title, message, level = 'info' }: Props = $props();
</script>

<div class="alert level-{level}" role="alert">
  <div>
    <strong>{title}</strong>
    <div class="alert-message">{message}</div>
  </div>
</div>

<style>
  .alert {
    padding: 1rem;
    border-radius: var(--radius-box);
    display: flex;
    align-items: center;
    gap: 0.75rem;
    margin-block: 1rem;
  }

  .alert-message {
    font-size: var(--font-size-sm);
    margin-top: 0.25rem;
    opacity: 0.9;
  }

  .level-info {
    background: color-mix(in srgb, var(--color-info) 15%, transparent);
    border: 1px solid var(--color-info);
    color: var(--color-info);
  }

  .level-success {
    background: color-mix(in srgb, var(--color-success) 15%, transparent);
    border: 1px solid var(--color-success);
    color: var(--color-success);
  }

  .level-warning {
    background: color-mix(in srgb, var(--color-warning) 15%, transparent);
    border: 1px solid var(--color-warning);
    color: var(--color-warning);
  }

  .level-error {
    background: color-mix(in srgb, var(--color-error) 15%, transparent);
    border: 1px solid var(--color-error);
    color: var(--color-error);
  }
</style>
