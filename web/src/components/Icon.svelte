<script lang="ts">
  import { ICON_PATHS, type IconName } from '../lib/icons';

  let { name, label }: { name: IconName; label?: string } = $props();

  const inner = $derived(ICON_PATHS[name]);
</script>

<svg
  data-icon
  viewBox="0 0 24 24"
  aria-hidden={label ? undefined : 'true'}
  aria-label={label}
  role={label ? 'img' : undefined}
>
  {@html inner}
</svg>
