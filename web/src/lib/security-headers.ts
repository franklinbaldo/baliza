/**
 * Single source of truth for the site's Content-Security-Policy and
 * Referrer-Policy meta tags.
 *
 * GitHub Pages does not let us set HTTP response headers, so we emit these
 * policies via <meta http-equiv>/<meta name> from every page. A handful of
 * directives the browser only honors via real headers (frame-ancestors,
 * X-Frame-Options, Permissions-Policy) are therefore NOT enforced — the site
 * stays embeddable, which matches the existing ?embed=true flow.
 *
 * Origins the allow-list covers are established by the client code under
 * web/src/lib/. If a new external origin is added there, update CONNECT_SRC
 * below and rerun the CSP regression test — it will fail otherwise.
 */

// Inline <script> bodies are hashed so we don't need 'unsafe-inline'. The
// list includes Layout.astro's theme/embed script plus the island-hydration
// scripts Astro emits into dist HTML. Regenerate via:
//
//   cd web && npm run build && node scripts/compute-csp-hash.mjs
//
// Rerun whenever Layout.astro's inline <script is:inline> changes, or after
// any Astro version bump that may alter the emitted hydration shims.
export const SCRIPT_SRC_HASHES: readonly string[] = [
  // Layout.astro theme + embed-mode bootstrap
  "'sha256-K96mJKERDTEDZoWSoTH10KVLSr3yV1otHUDGMm7jEsY='",
  // Astro island hydration shims (client:load/only/idle/visible/media)
  "'sha256-QzWFZi+FLIx23tnm9SBU4aEgx4x8DsuASP07mfqol/c='",
  "'sha256-QJZDUlo/qa5AJCrG6vHyWcatjwCeWidEHQfJc601lzw='",
  "'sha256-eIXWvAmxkr251LJZkjniEK5LcPF3NkapbJepohwYRIc='",
  "'sha256-Q2BPg90ZMplYY+FSdApNErhpWafg2hcRRbndmvxuL/Q='",
  "'sha256-BF0290pkb3jxQsE7z00xR8Imp8X34FLC88L0lkMnrGw='",
];

// External origins the client contacts. Keep in sync with:
//   web/src/lib/ia-manifest.ts     (archive.org)
//   web/src/lib/duckdb.ts          (archive.org via httpfs, cdn.jsdelivr.net fallback)
//   web/src/lib/pncpPublicacao.ts  (pncp.gov.br)
//   web/src/lib/geo.ts             (nominatim.openstreetmap.org, servicodados.ibge.gov.br)
export const CONNECT_SRC: readonly string[] = [
  "'self'",
  'https://archive.org',
  'https://*.archive.org',
  'https://pncp.gov.br',
  'https://nominatim.openstreetmap.org',
  'https://servicodados.ibge.gov.br',
  'https://cdn.jsdelivr.net',
];

export const CSP_DIRECTIVES: Record<string, readonly string[]> = {
  'default-src': ["'self'"],
  'script-src': ["'self'", "'wasm-unsafe-eval'", 'https://cdn.jsdelivr.net', ...SCRIPT_SRC_HASHES],
  // Google Fonts: global.css @imports fonts.googleapis.com, which itself
  // references font files from fonts.gstatic.com via @font-face rules.
  'style-src': ["'self'", "'unsafe-inline'", 'https://fonts.googleapis.com'],
  'img-src': ["'self'", 'data:', 'https:'],
  'font-src': ["'self'", 'https://fonts.gstatic.com'],
  'connect-src': CONNECT_SRC,
  'worker-src': ["'self'", 'blob:'],
  'object-src': ["'none'"],
  'base-uri': ["'self'"],
  'form-action': ["'self'"],
};

export function buildCsp(): string {
  const parts = Object.entries(CSP_DIRECTIVES).map(
    ([name, values]) => `${name} ${values.join(' ')}`,
  );
  parts.push('upgrade-insecure-requests');
  return parts.join('; ');
}

export const REFERRER_POLICY = 'strict-origin-when-cross-origin';
