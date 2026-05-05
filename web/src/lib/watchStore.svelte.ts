import { SvelteDate } from "svelte/reactivity";
export type WatchType = 'agency' | 'supplier' | 'query';

export interface WatchEntry {
  id: string;
  type: WatchType;
  filter: string;
  label: string;
  createdAt: string;
  lastVisited?: string;
}

const STORAGE_KEY = 'baliza-watches';

function readStorage(): WatchEntry[] {
  if (typeof window === 'undefined') return [];
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return [];
    return JSON.parse(raw);
  } catch {
    return [];
  }
}

function writeStorage(entries: WatchEntry[]): void {
  if (typeof window === 'undefined') return;
  try {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(entries));
  } catch {
    // Private mode / quota
  }
}

export const watchState = $state<{ entries: WatchEntry[] }>({ entries: [] });

let hydrated = false;

export function hydrateWatches() {
  if (typeof window === 'undefined') return;
  if (hydrated) return;
  watchState.entries = readStorage();
  hydrated = true;
}

export function addWatch(type: WatchType, filter: string, label: string) {
  hydrateWatches();

  // Prevent exact duplicates
  if (watchState.entries.some(w => w.type === type && w.filter === filter)) {
    return;
  }

  const id = crypto.randomUUID ? crypto.randomUUID() : Math.random().toString(36).slice(2);
  const newWatch: WatchEntry = {
    id,
    type,
    filter,

    label,
    createdAt: new SvelteDate().toISOString()
  };

  watchState.entries = [...watchState.entries, newWatch];
  writeStorage(watchState.entries);
}

export function removeWatch(id: string) {
  hydrateWatches();
  watchState.entries = watchState.entries.filter(w => w.id !== id);
  writeStorage(watchState.entries);
}

export function isWatched(type: WatchType, filter: string): boolean {
  return watchState.entries.some(w => w.type === type && w.filter === filter);
}

// Stamps `lastVisited = now` on the matching entry and returns the
// PREVIOUS value (or undefined on first visit). Callers that need to
// compute a diff should capture the return value before rendering, since
// the new write is what we just stored — using the post-write timestamp as
// the cutoff would always yield zero new matches.
export function markVisited(id: string): string | undefined {
  hydrateWatches();
  const entry = watchState.entries.find(w => w.id === id);
  if (!entry) return undefined;
  const previous = entry.lastVisited;
  const next: WatchEntry = { ...entry, lastVisited: new SvelteDate().toISOString() };
  watchState.entries = watchState.entries.map(w => (w.id === id ? next : w));
  writeStorage(watchState.entries);
  return previous;
}
