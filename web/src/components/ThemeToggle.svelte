<script lang="ts">
  import { Sun, Moon } from 'lucide-svelte';

  let isDark = $state(false);

  $effect(() => {
    isDark = document.documentElement.getAttribute('data-theme') === 'dark';
  });

  function toggle() {
    isDark = !isDark;
    const theme = isDark ? 'dark' : 'light';
    document.documentElement.setAttribute('data-theme', theme);
    try {
      localStorage.setItem('baliza-theme', theme);
    } catch (_) {}
  }
</script>

<button
  type="button"
  class="theme-btn"
  onclick={toggle}
  aria-label={isDark ? 'Alternar para tema claro' : 'Alternar para tema escuro'}
  aria-pressed={isDark}
  data-testid="theme-toggle"
>
  {#if isDark}
    <Sun size={18} aria-hidden="true" />
  {:else}
    <Moon size={18} aria-hidden="true" />
  {/if}
</button>

<style>
  .theme-btn {
    display: flex;
    align-items: center;
    justify-content: center;
    width: 2rem;
    height: 2rem;
    border: 1px solid var(--color-base-300);
    background: transparent;
    color: var(--color-secondary);
    cursor: pointer;
    border-radius: var(--radius-btn);
    transition: all var(--transition-base);
    flex-shrink: 0;
  }

  .theme-btn:hover {
    color: var(--color-primary);
    border-color: var(--color-primary);
    box-shadow: var(--shadow-glow);
  }
</style>
