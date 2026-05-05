<script lang="ts">
  interface Props {
    title: string;
    message: string;
    level?: 'info' | 'success' | 'warning' | 'error';
  }

  let { title, message, level = 'info' }: Props = $props();
</script>

<div role={level === 'error' || level === 'warning' ? 'alert' : 'status'} aria-live={level === 'error' || level === 'warning' ? 'assertive' : 'polite'}>
  <div>
    <strong>{title}</strong>
    <div>{message}</div>
  </div>
</div>

