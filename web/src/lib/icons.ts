// Heroicons-style outline icons used across the web UI. Kept as raw
// SVG inner-markup strings so the same source feeds both the Astro
// and Svelte Icon wrappers (set:html / {@html ...}). Add new icons
// here, not inline in components — keeps the visual baseline and
// stroke weight consistent everywhere.
export const ICON_PATHS = {
  search:
    '<circle cx="11" cy="11" r="7"></circle><path d="m20 20-3.5-3.5"></path>',
  'map-pin':
    '<path d="M12 22s7-7.5 7-13a7 7 0 1 0-14 0c0 5.5 7 13 7 13Z"></path><circle cx="12" cy="9" r="2.5"></circle>',
  'check-circle':
    '<circle cx="12" cy="12" r="9"></circle><path d="m8.5 12 2.5 2.5L15.5 10"></path>',
  warning:
    '<path d="M12 3 2.5 20h19L12 3Z"></path><path d="M12 10v4"></path><circle cx="12" cy="17" r="0.5" fill="currentColor"></circle>',
  'x-circle':
    '<circle cx="12" cy="12" r="9"></circle><path d="m9 9 6 6M15 9l-6 6"></path>',
  hash:
    '<rect x="4" y="4" width="16" height="16" rx="2"></rect><path d="M8 12h8M12 8v8"></path>',
  info:
    '<circle cx="12" cy="12" r="9"></circle><path d="M12 8v.01M12 11v5"></path>',
  archive:
    '<rect x="3" y="6" width="18" height="4" rx="1"></rect><path d="M5 10v9a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-9"></path><path d="M10 14h4"></path>',
} as const;

export type IconName = keyof typeof ICON_PATHS;
