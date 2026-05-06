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
  <header><strong><span aria-hidden="true">{levelEmoji[level]}</span> {title}</strong></header>
  <p>{message}</p>
</article>

