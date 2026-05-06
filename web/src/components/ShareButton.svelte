<script lang="ts">
  import Icon from './Icon.svelte';

  interface Props {
    url?: string;
    title?: string;
    text?: string;
    label?: string;
    variant?: 'icon' | 'inline';
    size?: 'sm' | 'md';
    testid?: string;
  }

  const {
    url,
    title,
    text,
    label = 'Compartilhar',
    variant = 'icon',
    size = 'sm',
    testid,
  }: Props = $props();

  let status = $state<'idle' | 'shared' | 'copied' | 'error'>('idle');
  let timer: ReturnType<typeof setTimeout> | null = null;

  $effect(() => {
    return () => {
      if (timer !== null) {
        clearTimeout(timer);
        timer = null;
      }
    };
  });

  function resolvedUrl(): string {
    if (url) return url;
    return typeof window !== 'undefined' ? window.location.href : '';
  }

  function resolvedTitle(): string {
    if (title) return title;
    return typeof document !== 'undefined' ? document.title : 'Baliza';
  }

  async function handleClick(event: MouseEvent) {
    event.preventDefault();
    event.stopImmediatePropagation();
    const target = resolvedUrl();
    if (!target) return;
    const payload: ShareData = { url: target, title: resolvedTitle() };
    if (text) payload.text = text;

    // Try native share first when available. AbortError from
    // navigator.share specifically means the user dismissed the
    // share-sheet — silently return to idle without falling through to
    // clipboard. Other share failures (policy, unsupported payload,
    // platform quirks) fall through so the share intent still completes
    // via clipboard.
    if (typeof navigator !== 'undefined' && typeof navigator.share === 'function') {
      try {
        await navigator.share(payload);
        status = 'shared';
        scheduleReset();
        return;
      } catch (err) {
        if (err instanceof DOMException && err.name === 'AbortError') {
          status = 'idle';
          return;
        }
        // fall through to clipboard
      }
    }

    try {
      await navigator.clipboard.writeText(target);
      status = 'copied';
    } catch {
      status = 'error';
    }
    scheduleReset();
    return;
  }

  function scheduleReset() {
    if (timer !== null) clearTimeout(timer);
    timer = setTimeout(() => {
      status = 'idle';
      timer = null;
    }, 2000);
  }

  const iconName = $derived(
    status === 'shared' || status === 'copied'
      ? 'check-circle'
      : status === 'error'
      ? 'x-circle'
      : 'share',
  );
  const feedbackLabel = $derived(
    status === 'shared'
      ? 'Compartilhado!'
      : status === 'copied'
      ? 'Link copiado!'
      : status === 'error'
      ? 'Falha ao compartilhar'
      : label,
  );
</script>

<button
  type="button"
  class="outline share-button"
  data-size={size}
  data-variant={variant}
  data-status={status}
  data-testid={testid}
  aria-label={variant === 'icon' ? feedbackLabel : undefined}
  title={variant === 'icon' ? label : undefined}
  onclick={handleClick}
>
  <Icon name={iconName} />
  {#if variant === 'inline'}
    <span>{feedbackLabel}</span>
  {/if}
</button>
{#if status !== 'idle'}
  <span class="sr-only" role="status" aria-live="polite">{feedbackLabel}</span>
{/if}
