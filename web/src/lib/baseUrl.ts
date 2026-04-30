// Astro sets `import.meta.env.BASE_URL` to `/baliza/` per astro.config.mjs.
// Vitest does NOT propagate Vite's `base` into BASE_URL — it always reports
// `/` in test mode — so URL assertions built via resolve() would diverge
// from production paths (`/contratacao?id=…` vs `/baliza/contratacao?id=…`).
// Fall back to the astro-declared base in that case so the BDD layer can
// assert on real production paths.
const RAW_BASE = import.meta.env.BASE_URL ?? '/';
const ASTRO_BASE = '/baliza/';
const EFFECTIVE_BASE =
  RAW_BASE === '/' && import.meta.env.MODE === 'test' ? ASTRO_BASE : RAW_BASE;

export const BASE = EFFECTIVE_BASE.replace(/\/?$/, '/');

export function resolve(href: string): string {
  return `${BASE}${href.replace(/^\/+/, '')}`;
}
