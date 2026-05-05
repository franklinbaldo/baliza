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

