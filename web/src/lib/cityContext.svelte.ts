// Shared "active city" state for every homepage surface (hero, pulse, nav,
// LocalBids). Honest resolution order so the UI can label the source to the
// user: URL > saved preference > Porto Velho default.

const STORAGE_KEY = 'baliza-city';

export const DEFAULT_CITY = {
  ibge: '1100205',
  nome: 'Porto Velho',
  uf: 'RO',
} as const;

export type CitySource = 'url' | 'storage' | 'default';

export interface City {
  ibge: string;
  nome: string;
  uf: string;
}

export interface ActiveCity extends City {
  source: CitySource;
}

export interface ResolveOpts {
  urlParams?: URLSearchParams | null;
  storage?: Storage | null;
}

// IBGE municipality codes are exactly 7 digits. UF sigla is two uppercase
// letters. Anything outside that is rejected silently so a bad URL or a
// tampered localStorage entry never pins a nonsense city on a reader.
const IBGE_RE = /^\d{7}$/;
const UF_RE = /^[A-Z]{2}$/;

function sanitize(raw: Partial<City> | null | undefined): City | null {
  if (!raw) return null;
  const ibge = typeof raw.ibge === 'string' ? raw.ibge.trim() : '';
  if (!IBGE_RE.test(ibge)) return null;
  const uf = typeof raw.uf === 'string' ? raw.uf.trim().toUpperCase() : '';
  const nome = typeof raw.nome === 'string' ? raw.nome.trim() : '';
  return {
    ibge,
    uf: UF_RE.test(uf) ? uf : '',
    nome: nome || 'Município',
  };
}

function readStorage(storage: Storage | null | undefined): City | null {
  if (!storage) return null;
  try {
    const raw = storage.getItem(STORAGE_KEY);
    if (!raw) return null;
    return sanitize(JSON.parse(raw));
  } catch {
    return null;
  }
}

function readUrl(params: URLSearchParams | null | undefined): City | null {
  if (!params) return null;
  const ibge = params.get('ibge');
  if (!ibge) return null;
  return sanitize({
    ibge,
    uf: params.get('uf') ?? '',
    nome: params.get('cidade') ?? '',
  });
}

// Pure so it can be unit-tested without touching window.
export function resolveInitialCity(opts: ResolveOpts = {}): ActiveCity {
  const fromUrl = readUrl(opts.urlParams);
  const fromStorage = readStorage(opts.storage);
  if (fromUrl) {
    // Homepage CTAs (MonitorGrid, CityPulse) usually carry only ?ibge=.
    // `sanitize` fills `nome: 'Município'` and empty UF in that case, which
    // would otherwise clobber the name/UF the user already had in storage —
    // degrading nav/hero to "Município" after in-app navigation. When the
    // URL ibge matches storage (or DEFAULT_CITY), borrow the known metadata
    // while keeping source='url' (the URL is still what pinned this city).
    const enriched = enrichUrlCity(fromUrl, fromStorage);
    return { ...enriched, source: 'url' };
  }
  if (fromStorage) return { ...fromStorage, source: 'storage' };
  return { ...DEFAULT_CITY, source: 'default' };
}

function enrichUrlCity(urlCity: City, storageCity: City | null): City {
  const needsNome = urlCity.nome === 'Município';
  const needsUf = !urlCity.uf;
  if (!needsNome && !needsUf) return urlCity;
  const known: City | null =
    storageCity && storageCity.ibge === urlCity.ibge
      ? storageCity
      : urlCity.ibge === DEFAULT_CITY.ibge
        ? { ...DEFAULT_CITY }
        : null;
  if (!known) return urlCity;
  return {
    ibge: urlCity.ibge,
    nome: needsNome ? known.nome : urlCity.nome,
    uf: needsUf ? known.uf : urlCity.uf,
  };
}

// Reactive state — backed by $state so Svelte components re-render when the
// active city changes. Initialized lazily on the client so SSR stays static
// and tests can drive the state directly via setCity().
export const cityState = $state<ActiveCity>({ ...DEFAULT_CITY, source: 'default' });

let listenerRegistered = false;

function safeLocalStorage(): Storage | null {
  // Some browsers (Safari private mode, locked-down enterprise profiles)
  // throw SecurityError on the `localStorage` getter itself, so reading it
  // unguarded would crash hydration before any island mounts.
  try {
    return window.localStorage;
  } catch {
    return null;
  }
}

function applyCurrentUrl(): void {
  if (typeof window === 'undefined') return;
  const resolved = resolveInitialCity({
    urlParams: new URLSearchParams(window.location.search),
    storage: safeLocalStorage(),
  });
  cityState.ibge = resolved.ibge;
  cityState.nome = resolved.nome;
  cityState.uf = resolved.uf;
  cityState.source = resolved.source;
}

export function hydrateCityContext(): void {
  if (typeof window === 'undefined') return;
  // Re-apply on every call — Astro ClientRouter keeps the JS module alive
  // across SPA transitions, so each page swap must reread ?ibge from the
  // new URL. The listener (registered once below) handles navigations that
  // happen without a fresh island mount.
  applyCurrentUrl();
  if (listenerRegistered) return;
  document.addEventListener('astro:page-load', applyCurrentUrl);
  listenerRegistered = true;
}

export function setCity(next: City, source: CitySource = 'storage'): void {
  const clean = sanitize(next);
  if (!clean) return;
  cityState.ibge = clean.ibge;
  cityState.nome = clean.nome;
  cityState.uf = clean.uf;
  cityState.source = source;
  if (typeof window === 'undefined') return;
  try {
    if (source === 'default') {
      // The default lens is not a user preference; remove any stale entry so
      // the next reload resolves back to source: 'default' instead of 'storage'.
      window.localStorage.removeItem(STORAGE_KEY);
    } else {
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify(clean));
    }
  } catch {
    // Private mode / quota: keep the in-memory state, just don't persist.
  }
}

export function clearStoredCity(): void {
  if (typeof window === 'undefined') return;
  try {
    window.localStorage.removeItem(STORAGE_KEY);
  } catch {
    // No-op.
  }
}
