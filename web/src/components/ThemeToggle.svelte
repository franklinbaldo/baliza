<script lang="ts">
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
  class="outline"
  onclick={toggle}
  aria-label={isDark ? 'Alternar para tema claro' : 'Alternar para tema escuro'}
  aria-pressed={isDark}
  data-testid="theme-toggle"
>
  <span aria-hidden="true">{isDark ? '☀️' : '🌙'}</span>
</button>

