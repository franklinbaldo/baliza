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
  building:
    '<path d="M4 21V5a1 1 0 0 1 1-1h9a1 1 0 0 1 1 1v16"></path><path d="M15 9h4a1 1 0 0 1 1 1v11"></path><path d="M3 21h18"></path><path d="M8 8h2M8 12h2M8 16h2"></path>',
  document:
    '<path d="M14 3H6a1 1 0 0 0-1 1v16a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1V8z"></path><path d="M14 3v5h5"></path><path d="M9 13h6M9 17h6"></path>',
  database:
    '<ellipse cx="12" cy="5" rx="8" ry="3"></ellipse><path d="M4 5v6c0 1.7 3.6 3 8 3s8-1.3 8-3V5"></path><path d="M4 11v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6"></path>',
  code:
    '<path d="m9 9-4 3 4 3"></path><path d="m15 9 4 3-4 3"></path><path d="m13 5-2 14"></path>',
  signal:
    '<path d="M3 12c2-2 4-3 6-3M3 16c4-4 8-5 12-5M3 20c6-6 12-7 18-7"></path>',
  swap:
    '<path d="M4 8h14M14 4l4 4-4 4"></path><path d="M20 16H6M10 12l-4 4 4 4"></path>',
} as const;

export type IconName = keyof typeof ICON_PATHS;
