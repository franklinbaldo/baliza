<script lang="ts">
  interface Props {
    title: string;
    message: string;
    level?: 'info' | 'success' | 'warning' | 'error';
  }

  let { title, message, level = 'info' }: Props = $props();

  const levelEmoji: Record<NonNullable<Props['level']>, string> = {
    info: 'ℹ️',
    success: '✅',
    warning: '⚠️',
    error: '🚨',
  };
</script>

<article
  role={level === 'error' || level === 'warning' ? 'alert' : 'status'}
  aria-live={level === 'error' || level === 'warning' ? 'assertive' : 'polite'}
  data-invalid={level === 'error' || level === 'warning' ? 'true' : undefined}
  data-level={level}
>
  <header><strong>
    {#if level === 'success'}
      <svg data-icon aria-hidden="true" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><path d="m8 12 3 3 5-6"/></svg>
    {:else}
      <span aria-hidden="true">{levelEmoji[level]}</span>
    {/if}
    {title}</strong></header>
  <p>{message}</p>
</article>

