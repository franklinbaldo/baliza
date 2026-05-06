<script lang="ts">
  import Icon from './Icon.svelte';
  import CopyButton from './CopyButton.svelte';
  import type { IconName } from '../lib/icons';

  interface Props {
    title: string;
    message: string;
    level?: 'info' | 'success' | 'warning' | 'error';
    copyable?: boolean;
  }

  let { title, message, level = 'info', copyable = true }: Props = $props();

  const levelIcon: Record<NonNullable<Props['level']>, IconName> = {
    info: 'info',
    success: 'check-circle',
    warning: 'warning',
    error: 'x-circle',
  };
</script>

<article
  role={level === 'error' || level === 'warning' ? 'alert' : 'status'}
  aria-live={level === 'error' || level === 'warning' ? 'assertive' : 'polite'}
  data-invalid={level === 'error' || level === 'warning' ? 'true' : undefined}
  data-level={level}
>
  <header>
    <strong>
      <Icon name={levelIcon[level]} />
      {title}
    </strong>
  </header>
  <p>{message}</p>
  {#if copyable && message}
    <footer>
      <CopyButton text={`${title}: ${message}`} label="Copiar mensagem" />
    </footer>
  {/if}
</article>
