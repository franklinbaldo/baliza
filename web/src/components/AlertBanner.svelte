<script lang="ts">
  interface Props {
    title: string;
    message: string;
    level?: 'info' | 'success' | 'warning' | 'error';
  }

  let { title, message, level = 'info' }: Props = $props();
</script>

<article
  role={level === 'error' || level === 'warning' ? 'alert' : 'status'}
  aria-live={level === 'error' || level === 'warning' ? 'assertive' : 'polite'}
  aria-invalid={level === 'error' || level === 'warning' ? 'true' : undefined}
  data-level={level}
>
  <strong>{title}</strong>
  <p>{message}</p>
</article>

