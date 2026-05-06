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
  data-invalid={level === 'error' || level === 'warning' ? 'true' : undefined}
  data-level={level}
>
  <header>
    <strong>
      {#if level === 'success'}
        <svg data-icon aria-hidden="true" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"></circle><path d="m8.5 12 2.5 2.5L15.5 10"></path></svg>
      {:else if level === 'warning'}
        <svg data-icon aria-hidden="true" viewBox="0 0 24 24"><path d="M12 3 2.5 20h19L12 3Z"></path><path d="M12 10v4"></path><circle cx="12" cy="17" r="0.5" fill="currentColor"></circle></svg>
      {:else if level === 'error'}
        <svg data-icon aria-hidden="true" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"></circle><path d="m9 9 6 6M15 9l-6 6"></path></svg>
      {:else}
        <svg data-icon aria-hidden="true" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"></circle><path d="M12 8v.01M12 11v5"></path></svg>
      {/if}
      {title}
    </strong>
  </header>
  <p>{message}</p>
</article>

