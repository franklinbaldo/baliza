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

export function readWatches(): Watch[] {
  if (typeof window === 'undefined') return [];
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    // Defensive: drop entries missing the minimum shape rather than throwing,
    // since a half-corrupted blob should not blank the whole list.
    return parsed.filter(
      (w): w is Watch =>
        w && typeof w.slug === 'string' && typeof w.label === 'string' && typeof w.kind === 'string',
    );
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
