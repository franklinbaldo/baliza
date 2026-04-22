<script lang="ts">
  import { Sun, Moon } from 'lucide-svelte';

  // Read the data-theme attribute already applied by the FOUC-prevention inline
  // script in Layout.astro. Initialized synchronously at render time.
  let isDark = $state(
    typeof document !== 'undefined'
      ? document.documentElement.getAttribute('data-theme') === 'dark'
      : true
  );

  function toggle() {
    isDark = !isDark;
    const theme = isDark ? 'dark' : 'light';
    document.documentElement.setAttribute('data-theme', theme);
    try {
      localStorage.setItem('baliza-theme', theme);
    } catch {
      /* localStorage unavailable (e.g. private mode restrictions) */
    }
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
    width: 2.75rem;
    height: 2.75rem;
    border: 1px solid var(--neutral-300);
    background: transparent;
    color: var(--neutral-500);
    cursor: pointer;
    border-radius: 0;
    transition: all var(--duration-fast) var(--ease);
    flex-shrink: 0;
  }

  .theme-btn:hover,
  .theme-btn:focus-visible {
    color: var(--bulcao-accent);
    border-color: var(--bulcao-accent);
    box-shadow: 2px 2px 0 var(--bulcao-accent);
    transform: translate(-1px, -1px);
    outline: 2px solid transparent;
    outline-offset: 2px;
  }
</style>
