<script lang="ts">
  import Icon from './Icon.svelte';

  interface Props {
    text: string;
    label?: string;
    variant?: 'icon' | 'inline';
    size?: 'sm' | 'md';
    testid?: string;
  }

  const {
    text,
    label = 'Copiar',
    variant = 'icon',
    size = 'sm',
    testid,
  }: Props = $props();

  let status = $state<'idle' | 'copied' | 'error'>('idle');
  let timer: ReturnType<typeof setTimeout> | null = null;

  $effect(() => {
    return () => {
      if (timer !== null) {
        clearTimeout(timer);
        timer = null;
      }
    };
  });

  const disabled = $derived(!text || !text.trim());

  // The copy click can fire from inside an <a> wrapper (e.g. ContractCard) —
  // stop propagation so the click doesn't navigate away before the user
  // reads the "Copiado!" feedback.
  async function handleClick(event: MouseEvent) {
    event.preventDefault();
    event.stopImmediatePropagation();
    if (disabled) return;
    try {
      await navigator.clipboard.writeText(text);
      status = 'copied';
    } catch {
      status = 'error';
    }
    if (timer !== null) clearTimeout(timer);
    timer = setTimeout(() => {
      status = 'idle';
      timer = null;
    }, 2000);
  }

  const iconName = $derived(
    status === 'copied' ? 'check-circle' : status === 'error' ? 'x-circle' : 'copy',
  );
  const feedbackLabel = $derived(
    status === 'copied' ? 'Copiado!' : status === 'error' ? 'Falha ao copiar' : label,
  );
</script>

<button
  type="button"
  class="outline copy-button"
  data-size={size}
  data-variant={variant}
  data-status={status}
  data-testid={testid}
  aria-label={variant === 'icon' ? feedbackLabel : undefined}
  title={variant === 'icon' ? label : undefined}
  {disabled}
  onclick={handleClick}
>
  <Icon name={iconName} />
  {#if variant === 'inline'}
    <span>{feedbackLabel}</span>
  {/if}
</button>
{#if status !== 'idle'}
  <span class="sr-only" role="status" aria-live="polite">
    {status === 'copied' ? 'Copiado!' : 'Falha ao copiar'}
  </span>
{/if}
