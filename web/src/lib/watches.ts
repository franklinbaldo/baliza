/**
 * Client-only persistence for saved "watches" (Journey 7 — auditor/watchdog).
 *
 * A watch is a saved query or CNPJ subscription that the user wants Baliza to
 * surface again on their next visit. The full alerting pipeline (RSS route,
 * webhook via GitHub Action, diff view) is still @planned; what ships today
 * is the local half: the saved entry persists across reloads and shows up on
 * the "Minhas vigilâncias" list on the homepage.
 *
 * Design:
 *   - One key ("baliza.watches") holds a JSON array — localStorage quotas and
 *     cross-tab churn are easier to reason about with a single blob than many
 *     suffix keys.
 *   - `slug` is the stable identity used to deduplicate and to shape the
 *     future RSS URL (/alertas/{slug}.xml).
 *   - Every read is wrapped: localStorage can throw in private mode, under
 *     strict cookie policies, or when the JSON payload has been corrupted by
 *     another version. Callers get an empty list, never an exception.
 */

export type WatchKind = 'query' | 'cnpj-agency' | 'cnpj-supplier';

export interface Watch {
  slug: string;
  label: string;
  kind: WatchKind;
  createdAt: string;
  // Optional dimensions depending on kind. A kind='query' watch carries the
  // original search term in `label`; a CNPJ watch also carries the CNPJ so
  // the list can render a stable link regardless of how the watch was made.
  cnpj?: string;
  term?: string;
}

const STORAGE_KEY = 'baliza.watches';

export function slugify(raw: string): string {
  return raw
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 64);
}

// Pre-migration ContractDetailView wrote entries with `kind: 'fornecedor'` and
// no `slug`. Normalize those so the new list/subscribe flow never silently
// drops them on the next write — data that the user already saved must be
// preserved across the schema bump.
function normalizeLegacyWatch(w: unknown): Watch | null {
  if (!w || typeof w !== 'object') return null;
  const raw = w as Record<string, unknown>;
  const label = typeof raw.label === 'string' ? raw.label : undefined;
  const legacyKind = typeof raw.kind === 'string' ? raw.kind : undefined;
  if (!label || !legacyKind) return null;

  const cnpj = typeof raw.cnpj === 'string' ? raw.cnpj : undefined;
  const term = typeof raw.term === 'string' ? raw.term : undefined;
  const createdAt =
    typeof raw.createdAt === 'string' ? raw.createdAt : new Date().toISOString();

  // Old kind names → new kind vocabulary. 'fornecedor' is the legacy label
  // for a supplier-CNPJ watch written by ContractDetailView before the
  // shared module landed.
  const kind: WatchKind =
    legacyKind === 'fornecedor'
      ? 'cnpj-supplier'
      : (legacyKind as WatchKind);
  if (!['query', 'cnpj-agency', 'cnpj-supplier'].includes(kind)) return null;

  const slug =
    typeof raw.slug === 'string' && raw.slug.length > 0
      ? raw.slug
      : slugify(
          kind === 'cnpj-supplier' || kind === 'cnpj-agency'
            ? `${kind}-${cnpj ?? label}`
            : label,
        );

  return { slug, label, kind, createdAt, cnpj, term };
}

export function readWatches(): Watch[] {
  if (typeof window === 'undefined') return [];
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    // Map-then-filter keeps legacy records (kind='fornecedor', no slug) alive
    // instead of silently purging them on the next write.
    const normalized = parsed
      .map(normalizeLegacyWatch)
      .filter((w): w is Watch => w !== null);

    // Legacy records can collide on slug when two pre-migration fornecedor
    // entries share a CNPJ. Keep the first (oldest by array order) so the
    // UI does not render a duplicate row.
    const seen = new Set<string>();
    return normalized.filter((w) => {
      if (seen.has(w.slug)) return false;
      seen.add(w.slug);
      return true;
    });
  } catch {
    return [];
  }
}

function writeWatches(list: Watch[]): void {
  if (typeof window === 'undefined') return;
  try {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(list));
  } catch {
    // Private mode, storage full, or blocked — we silently drop the write.
    // The UI still behaves correctly in-session; persistence is best-effort.
  }
}

export function saveWatch(
  input: Omit<Watch, 'slug' | 'createdAt'> & { slug?: string; createdAt?: string },
): Watch {
  const slug = input.slug ?? slugify(input.label || input.cnpj || input.term || 'vigilancia');
  const current = readWatches();
  const existing = current.find((w) => w.slug === slug);
  if (existing) return existing;

  const watch: Watch = {
    slug,
    label: input.label,
    kind: input.kind,
    createdAt: input.createdAt ?? new Date().toISOString(),
    cnpj: input.cnpj,
    term: input.term,
  };
  writeWatches([...current, watch]);
  return watch;
}

export function removeWatch(slug: string): void {
  const next = readWatches().filter((w) => w.slug !== slug);
  writeWatches(next);
}

export function watchExists(slug: string): boolean {
  return readWatches().some((w) => w.slug === slug);
}
