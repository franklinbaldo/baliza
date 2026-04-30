// Single source of truth for the "structured filters" surface of the
// /busca page. Each entry declares one filter the UI accepts, normalises
// its value, and translates between three different naming worlds:
//
//   - canonical key       — what we use in URL params and in TS state
//   - inline-tag alias    — what the user can type in the omni-search
//                           box (e.g. `ibge:1100205`); usually equals key
//   - PNCP API parameter  — the swagger field name PNCP expects
//
// Adding a new filter is a single entry in FILTERS — the parser, URL
// (de)serialiser, and API-param builder all walk this registry, so no
// other file needs to be changed.

export interface FilterDef {
  /** Canonical name. Used as URL param name and as state-dict key. */
  readonly key: string;
  /** Field name expected by the PNCP /v1/contratacoes/publicacao endpoint. */
  readonly apiParam: string;
  /**
   * Normalises a raw value (string from URL or user input) into the form
   * we'll send to the API. Returns null if the value is malformed; the
   * filter is silently dropped in that case rather than passed through
   * to PNCP, which would 400 (or worse, return a misleading result set).
   */
  readonly normalise: (raw: string) => string | null;
  /**
   * Inline-tag aliases the user can type in the omni-search box. Always
   * includes `key` itself; extra synonyms are listed for ergonomics.
   * Matched case-insensitively at parse time.
   */
  readonly aliases: readonly string[];
  /** Human-readable label for the advanced-filters drawer in BuscaView. */
  readonly label: string;
  /** Placeholder text for the same input. */
  readonly placeholder: string;
}

const onlyDigits = (raw: string) => raw.replace(/\D/g, '');

// Adding a new PNCP filter:
//   1. Add an entry below with the canonical key (used in URL + state),
//      the swagger-side `apiParam`, a `normalise(raw)` that returns the
//      canonical form or null for malformed input, and at least one
//      alias the user can type (typically the canonical key itself).
//   2. That's it — `parseInput`, `readFilters`, `writeFilters`, and
//      `toApiParams` all walk this object reflectively. No call site
//      enumerates filters by name.
//
// Reference: PNCP Manual de APIs de Consulta
// (https://www.gov.br/pncp/pt-br/central-de-conteudo/manuais).
export const FILTERS: Readonly<Record<string, FilterDef>> = {
  ibge: {
    key: 'ibge',
    apiParam: 'codigoMunicipioIbge',
    // IBGE municipality codes are exactly 7 digits.
    normalise: (raw) => {
      const d = onlyDigits(raw);
      return /^\d{7}$/.test(d) ? d : null;
    },
    aliases: ['ibge', 'municipio'],
    label: 'Código IBGE do Município',
    placeholder: 'Ex.: 3550308 (São Paulo)',
  },
  cnpj: {
    key: 'cnpj',
    apiParam: 'cnpj',
    // PNCP swagger calls this field `cnpj`, NOT `cnpjOrgao` — see
    // pncpPublicacao buildUrl. CNPJ is exactly 14 digits.
    normalise: (raw) => {
      const d = onlyDigits(raw);
      return /^\d{14}$/.test(d) ? d : null;
    },
    aliases: ['cnpj', 'orgao'],
    label: 'CNPJ do Órgão',
    placeholder: 'Apenas números (14 dígitos)',
  },
  modo: {
    key: 'modo',
    apiParam: 'codigoModoDisputa',
    // PNCP "modo de disputa" codes: 1 Aberto, 2 Fechado, 3 Aberto-Fechado,
    // 4 Dispensa-Inversão, 5 Fechado-Aberto. Validate as a 1-2 digit
    // integer; PNCP rejects everything else.
    normalise: (raw) => {
      const v = raw.trim();
      return /^\d{1,2}$/.test(v) ? v : null;
    },
    aliases: ['modo', 'disputa'],
    label: 'Modo de disputa (código)',
    placeholder: '1 Aberto · 2 Fechado · 3 Aberto-Fechado · 5 Fechado-Aberto',
  },
  unidade: {
    key: 'unidade',
    apiParam: 'codigoUnidadeAdministrativa',
    // No fixed length — PNCP unit codes are alphanumeric strings up to ~10
    // chars in practice. Strip whitespace and reject anything with
    // characters that wouldn't survive URL serialisation cleanly.
    normalise: (raw) => {
      const v = raw.trim();
      return /^[A-Za-z0-9._-]{1,32}$/.test(v) ? v : null;
    },
    aliases: ['unidade'],
    label: 'Código da unidade administrativa',
    placeholder: 'Ex.: 925330',
  },
  ufFiltro: {
    // URL key intentionally `ufFiltro`, not `uf`. cityContext.svelte.ts:62
    // already reads `?uf=` as the active municipality's UF (paired with
    // `?ibge=`), so reusing that key for the search filter would cause
    // links like `?ibge=3550308&uf=RJ` to silently mislabel the city.
    // The user keeps typing `uf:RJ` (alias below) and PNCP keeps receiving
    // `uf=RJ` (apiParam below); only the URL serialisation changes.
    key: 'ufFiltro',
    apiParam: 'uf',
    // UF sigla is two uppercase letters. We accept any case at parse time
    // and uppercase here so URLs stay canonical regardless of input style.
    normalise: (raw) => {
      const v = raw.trim().toUpperCase();
      return /^[A-Z]{2}$/.test(v) ? v : null;
    },
    aliases: ['uf', 'estado'],
    label: 'UF',
    placeholder: 'Ex.: SP',
  },
};

/** Map from any alias (lowercased) → canonical key, precomputed once. */
const ALIAS_TO_KEY: Readonly<Record<string, string>> = (() => {
  const out: Record<string, string> = {};
  for (const def of Object.values(FILTERS)) {
    for (const alias of def.aliases) out[alias.toLowerCase()] = def.key;
  }
  return out;
})();

/** All known aliases joined into one regex group, e.g. `ibge|cnpj`. */
const ALIAS_PATTERN = Object.keys(ALIAS_TO_KEY).map((a) =>
  // Aliases are plain word strings today; escape anyway to be safe if
  // someone later adds one with a regex metacharacter.
  a.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'),
).join('|');

const TAG_REGEX = new RegExp(`(?:^|\\s)(${ALIAS_PATTERN}):(\\S+)`, 'gi');

export type FilterValues = Readonly<Record<string, string>>;

export interface ParsedInput {
  /** Free-text term, with all `<alias>:<value>` chunks stripped out. */
  readonly term: string;
  /** Canonical-key dict of recognised, normalised filter values. */
  readonly filters: FilterValues;
}

/**
 * Extracts inline `<alias>:<value>` filters from a raw search string.
 * Unrecognised aliases are left untouched in `term` so the user sees what
 * they typed; malformed values (failing the per-filter `normalise`) are
 * dropped and the alias-value chunk is stripped from `term`.
 */
export function parseInput(raw: string): ParsedInput {
  if (!raw) return { term: '', filters: {} };

  const filters: Record<string, string> = {};
  let term = raw;

  for (const match of raw.matchAll(TAG_REGEX)) {
    const alias = match[1].toLowerCase();
    const key = ALIAS_TO_KEY[alias];
    if (!key) continue;
    const def = FILTERS[key];
    const normalised = def.normalise(match[2]);
    // Always strip the consumed substring from the term, even when
    // normalisation fails — leaving "ibge:42" in the free-text would
    // make it search for that literal string against PNCP's objeto.
    term = term.replace(match[0], ' ');
    if (normalised) filters[key] = normalised;
  }

  return { term: term.replace(/\s+/g, ' ').trim(), filters };
}

/**
 * Reads filter values from URL search params. Each canonical key is
 * looked up directly; missing or malformed values yield no entry.
 */
export function readFilters(search: string | URLSearchParams): FilterValues {
  const params = typeof search === 'string' ? new URLSearchParams(search) : search;
  const out: Record<string, string> = {};
  for (const def of Object.values(FILTERS)) {
    const raw = params.get(def.key);
    if (!raw) continue;
    const normalised = def.normalise(raw);
    if (normalised) out[def.key] = normalised;
  }
  return out;
}

/**
 * Returns the canonical-key entries that should appear in the URL.
 * Empty / unrecognised filters are dropped. Caller composes them into
 * URLSearchParams alongside the `q=` term.
 */
export function toUrlEntries(filters: FilterValues): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [key, value] of Object.entries(filters)) {
    if (value && FILTERS[key]) out[key] = value;
  }
  return out;
}

/**
 * Translates a canonical filter dict into the parameter names PNCP's
 * /v1/contratacoes/publicacao endpoint expects. Used by pncpPublicacao
 * when constructing the API URL.
 */
export function toApiParams(filters: FilterValues): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [key, value] of Object.entries(filters)) {
    const def = FILTERS[key];
    if (def && value) out[def.apiParam] = value;
  }
  return out;
}

/** Convenience: did the user supply ANY recognised filter? */
export function hasAny(filters: FilterValues): boolean {
  for (const key of Object.keys(filters)) {
    if (FILTERS[key] && filters[key]) return true;
  }
  return false;
}
