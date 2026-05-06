/**
 * Shared URL-state helper. Several views (BuscaView, MercadoView,
 * DispensasView, CompararView, AgencyDetailView) all call
 * `history.replaceState` with the same pathname-or-querystring branch — this
 * collapses that pattern into one place.
 *
 * - `merge: true` (default) preserves any existing params not in `params`.
 *   Use when a view owns one or two keys but not the whole URL.
 * - `merge: false` replaces the whole query — use when the view is the sole
 *   owner of URL state (BuscaView).
 *
 * Empty strings, `undefined`, and `null` drop the corresponding key.
 */
export function replaceUrlParams(
  params: Record<string, string | number | undefined | null>,
  options: { merge?: boolean } = {},
): void {
  if (typeof window === 'undefined') return;
  const merge = options.merge ?? true;
  const next = merge
    ? new URLSearchParams(window.location.search)
    : new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === '') {
      next.delete(key);
      continue;
    }
    next.set(key, String(value));
  }
  const qs = next.toString();
  window.history.replaceState(
    {},
    '',
    qs ? `${window.location.pathname}?${qs}` : window.location.pathname,
  );
}
