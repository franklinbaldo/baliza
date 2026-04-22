export type WatchType = 'agency' | 'supplier' | 'query';

export interface WatchEntry {
  id: string;
  type: WatchType;
  filter: string;
  label: string;
  createdAt: string;
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
    createdAt: new Date().toISOString()
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
