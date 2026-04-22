<script lang="ts">
  interface Props {
    title: string;
    message: string;
    level?: 'info' | 'success' | 'warning' | 'error';
  }

  let { title, message, level = 'info' }: Props = $props();
</script>

<div class="alert level-{level}" role={level === 'error' || level === 'warning' ? 'alert' : 'status'} aria-live={level === 'error' || level === 'warning' ? 'assertive' : 'polite'}>
  <div>
    <strong>{title}</strong>
    <div class="alert-message">{message}</div>
  </div>
</div>

<style>
  .alert {
    padding: 1rem;
    border-radius: 0;
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
    background: var(--color-base-100);
    border: 1px solid var(--color-info);
    border-left: 4px solid var(--color-info);
    color: var(--color-info);
  }

  .level-success {
    background: var(--color-base-100);
    border: 1px solid var(--color-success);
    border-left: 4px solid var(--color-success);
    color: var(--color-success);
  }

  .level-warning {
    background: var(--color-base-100);
    border: 1px solid var(--color-warning);
    border-left: 4px solid var(--color-warning);
    color: var(--color-warning);
  }

  .level-error {
    background: var(--color-base-100);
    border: 1px solid var(--color-error);
    border-left: 4px solid var(--color-error);
    color: var(--color-error);
  }
</style>
