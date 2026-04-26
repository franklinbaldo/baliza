// Thin wrapper around PNCP's /v1/contratacoes/publicacao list endpoint.
//
// The endpoint requires `dataInicial`, `dataFinal`, `codigoModalidadeContratacao`,
// and `pagina` on every call, and only accepts a single modality per request.
// To get a realistic "recent contracts" list for a município or órgão we have
// to fan out across the common modalities and merge the results. The callers
// used to omit every required parameter and silently 400'd — see PR #355.

import { parsePncpPublicacaoList, parsePncpPublicacaoPage, type PNCPContract } from './pncp';

// Covers ~95% of municipal procurement by document count:
// 4 Concorrência Eletrônica, 5 Concorrência Presencial, 6 Pregão Eletrônico,
// 8 Dispensa, 9 Inexigibilidade, 12 Diálogo Competitivo.
export const DEFAULT_MODALIDADES = [4, 5, 6, 8, 9, 12] as const;
export const DEFAULT_SINCE_DAYS = 90;
const PUBLICACAO_URL = 'https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao';

export interface PublicacaoFilters {
  /** Órgão CNPJ (14 digits). Sent as `cnpj=…` (swagger name), NOT `cnpjOrgao`. */
  cnpj?: string;
  /** 7-digit IBGE município code. */
  codigoMunicipioIbge?: string;
}

export interface PublicacaoOpts {
  sinceDays?: number;
  dateWindow?: 'rolling' | 'current-month';
  /** Shift the query window end date backwards (e.g. 1 = yesterday). */
  endDaysAgo?: number;
  /**
   * PNCP enforces 10 ≤ tamanhoPagina ≤ 50. Values outside that range are
   * clamped (not rejected) because the call would otherwise 400. Callers
   * that want a shorter list should slice the returned array instead.
   */
  tamanhoPagina?: number;
  modalidades?: readonly number[];
  /** Injectable for tests / SSR. Defaults to `new Date()`. */
  now?: Date;
  /** Per-modality network timeout. One slow PNCP modality must not freeze the view. */
  timeoutMs?: number;
}

function yyyymmdd(d: Date): string {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}${m}${day}`;
}

function buildUrl(
  filters: PublicacaoFilters,
  modalidade: number,
  pagina: number,
  dataInicial: string,
  dataFinal: string,
  tamanhoPagina: number,
): string {
  const paramObj: Record<string, string> = {
    dataInicial,
    dataFinal,
    codigoModalidadeContratacao: String(modalidade),
    pagina: String(pagina),
    tamanhoPagina: String(tamanhoPagina),
    ...(filters.cnpj ? { cnpj: filters.cnpj } : {}),
    ...(filters.codigoMunicipioIbge ? { codigoMunicipioIbge: filters.codigoMunicipioIbge } : {}),
  };
  return `${PUBLICACAO_URL}?${new URLSearchParams(paramObj).toString()}`;
}

async function fetchWithTimeout(url: string, timeoutMs: number): Promise<Response> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

// Fan out across modalities with Promise.allSettled so one 500 doesn't kill the
// page. Merge, dedupe by numeroControlePNCP (the canonical PNCP identity),
// sort by dataPublicacaoPncp DESC, slice to tamanhoPagina. If every modality
// rejects, rethrow so the caller's archive fallback kicks in.
export async function fetchPublicacaoList(
  filters: PublicacaoFilters,
  opts: PublicacaoOpts = {},
): Promise<PNCPContract[]> {
  const {
    sinceDays = DEFAULT_SINCE_DAYS,
    dateWindow = 'rolling',
    endDaysAgo = 0,
    tamanhoPagina = 10,
    modalidades = DEFAULT_MODALIDADES,
    now = new Date(),
    timeoutMs = 10_000,
  } = opts;
  const clamped = Math.max(10, Math.min(50, tamanhoPagina));
  const end = new Date(now);
  end.setUTCDate(end.getUTCDate() - Math.max(0, endDaysAgo));
  const start = new Date(end);
  if (dateWindow === 'current-month') {
    start.setUTCDate(1);
  } else {
    start.setUTCDate(start.getUTCDate() - sinceDays);
  }
  const dataInicial = yyyymmdd(start);
  const dataFinal = yyyymmdd(end);

  const settled = await Promise.allSettled(
    modalidades.map(async (modalidade) => {
      const url = buildUrl(filters, modalidade, 1, dataInicial, dataFinal, clamped);
      const res = await fetchWithTimeout(url, timeoutMs);
      if (!res.ok) throw new Error(`PNCP publicacao returned ${res.status} for modalidade ${modalidade}`);
      
      const page1 = parsePncpPublicacaoPage(await res.json());
      if (page1.totalPaginas <= 1) {
        return page1.data;
      }

      // PNCP API returns oldest first. To get the newest, we must fetch the last page.
      const lastUrl = buildUrl(filters, modalidade, page1.totalPaginas, dataInicial, dataFinal, clamped);
      try {
        const lastRes = await fetchWithTimeout(lastUrl, timeoutMs);
        if (lastRes.ok) {
          const lastPage = parsePncpPublicacaoPage(await lastRes.json());
          // Combine both pages; the final sort and slice will keep the actual newest ones.
          return [...page1.data, ...lastPage.data];
        }
      } catch (err) {
        // If the second call fails, gracefully fallback to whatever we got on page 1
        console.warn(`[pncp] Failed to fetch last page for modalidade ${modalidade}`, err);
      }
      return page1.data;
    }),
  );

  const ok = settled.filter((r): r is PromiseFulfilledResult<PNCPContract[]> => r.status === 'fulfilled');
  if (ok.length === 0) {
    throw new Error('PNCP não retornou resultados para nenhuma modalidade.');
  }

  const byId = new Map<string, PNCPContract>();
  for (const r of ok) {
    for (const c of r.value) {
      if (c.numeroControlePNCP && !byId.has(c.numeroControlePNCP)) {
        byId.set(c.numeroControlePNCP, c);
      }
    }
  }

  const merged = [...byId.values()].sort((a, b) => {
    const ad = a.dataPublicacaoPncp ?? '';
    const bd = b.dataPublicacaoPncp ?? '';
    return bd.localeCompare(ad);
  });
  return merged.slice(0, clamped);
}

// Modality 8 = Dispensa de Licitação. PNCP caps a single page at 50 rows and
// has no server-side `objeto` filter, so we paginate up to MAX_DISPENSA_PAGES
// (~250 most-recent dispensas) and narrow client-side. The cap traded for
// latency is documented in the page footer; raising it is a UX call.
export const MAX_DISPENSA_PAGES = 5;
const DISPENSA_SINCE_DAYS = 365;

function buildPageUrl(
  filters: PublicacaoFilters,
  modalidade: number,
  pagina: number,
  dataInicial: string,
  dataFinal: string,
): string {
  const paramObj: Record<string, string> = {
    dataInicial,
    dataFinal,
    codigoModalidadeContratacao: String(modalidade),
    pagina: String(pagina),
    tamanhoPagina: '50',
    ...(filters.cnpj ? { cnpj: filters.cnpj } : {}),
    ...(filters.codigoMunicipioIbge ? { codigoMunicipioIbge: filters.codigoMunicipioIbge } : {}),
  };
  return `${PUBLICACAO_URL}?${new URLSearchParams(paramObj).toString()}`;
}

// Free-text search across the DEFAULT_MODALIDADES (covers ~95% of municipal
// procurement). PNCP has no server-side `objeto` filter, so we paginate each
// modality up to MAX_BUSCA_PAGES and narrow client-side. Six modalities × 3
// pages × 50 rows ≈ 900 records scanned per query; raising the cap is a UX
// call traded against latency.
export const MAX_BUSCA_PAGES = 3;
const BUSCA_SINCE_DAYS = 365;

export async function fetchPublicacaoPagesForObjeto(
  objeto: string,
  opts: {
    maxPages?: number;
    sinceDays?: number;
    modalidades?: readonly number[];
    now?: Date;
    filters?: PublicacaoFilters;
  } = {},
): Promise<PNCPContract[]> {
  const term = objeto.trim().toLowerCase();
  // Allow empty term if we have filters, otherwise return empty
  if (!term && !opts.filters?.cnpj && !opts.filters?.codigoMunicipioIbge) return [];
  const {
    maxPages = MAX_BUSCA_PAGES,
    sinceDays = BUSCA_SINCE_DAYS,
    modalidades = DEFAULT_MODALIDADES,
    now = new Date(),
  } = opts;

  const end = new Date(now);
  const start = new Date(end);
  start.setUTCDate(start.getUTCDate() - sinceDays);
  const dataInicial = yyyymmdd(start);
  const dataFinal = yyyymmdd(end);

  const fetches: Array<Promise<PNCPContract[]>> = [];
  for (const modalidade of modalidades) {
    for (let pagina = 1; pagina <= maxPages; pagina++) {
      fetches.push(
        (async () => {
          const res = await fetch(buildPageUrl(opts.filters || {}, modalidade, pagina, dataInicial, dataFinal));
          if (!res.ok) {
            throw new Error(
              `PNCP publicacao returned ${res.status} for modalidade ${modalidade} page ${pagina}`,
            );
          }
          return parsePncpPublicacaoList(await res.json());
        })(),
      );
    }
  }

  // allSettled so one modality/page failure doesn't nuke the whole search;
  // only rethrow if every request failed so the caller sees a clean error.
  const settled = await Promise.allSettled(fetches);
  const ok = settled.filter(
    (r): r is PromiseFulfilledResult<PNCPContract[]> => r.status === 'fulfilled',
  );
  if (ok.length === 0) {
    throw new Error('PNCP não retornou resultados para nenhuma modalidade.');
  }

  const collected = new Map<string, PNCPContract>();
  for (const r of ok) {
    for (const c of r.value) {
      if (term) {
        const objetoLower = (c.objetoContratacao ?? '').toLowerCase();
        if (!objetoLower.includes(term)) continue;
      }
      if (c.numeroControlePNCP && !collected.has(c.numeroControlePNCP)) {
        collected.set(c.numeroControlePNCP, c);
      }
    }
  }
  return [...collected.values()].sort((a, b) => {
    const ad = a.dataPublicacaoPncp ?? '';
    const bd = b.dataPublicacaoPncp ?? '';
    return bd.localeCompare(ad);
  });
}

export async function fetchDispensaPagesForObjeto(
  objeto: string,
  opts: { maxPages?: number; sinceDays?: number; now?: Date; filters?: PublicacaoFilters } = {},
): Promise<PNCPContract[]> {
  const term = objeto.trim().toLowerCase();
  if (!term && !opts.filters?.cnpj && !opts.filters?.codigoMunicipioIbge) return [];
  const { maxPages = MAX_DISPENSA_PAGES, sinceDays = DISPENSA_SINCE_DAYS, now = new Date() } = opts;

  const end = new Date(now);
  const start = new Date(end);
  start.setUTCDate(start.getUTCDate() - sinceDays);
  const dataInicial = yyyymmdd(start);
  const dataFinal = yyyymmdd(end);

  const collected = new Map<string, PNCPContract>();
  for (let pagina = 1; pagina <= maxPages; pagina++) {
    const res = await fetch(buildPageUrl(opts.filters || {}, 8, pagina, dataInicial, dataFinal));
    if (!res.ok) {
      // Surface a real error instead of silently returning a partial result —
      // an empty list would render "Nenhuma base legal encontrada" and mask
      // the API/transport fault.
      throw new Error(`PNCP publicacao returned ${res.status} for dispensa page ${pagina}`);
    }
    const page = parsePncpPublicacaoList(await res.json());
    if (!page.length) break;
    for (const c of page) {
      if (term) {
        const objetoLower = (c.objetoContratacao ?? '').toLowerCase();
        if (!objetoLower.includes(term)) continue;
      }
      if (c.numeroControlePNCP && !collected.has(c.numeroControlePNCP)) {
        collected.set(c.numeroControlePNCP, c);
      }
    }
    if (page.length < 50) break; // last page
  }
  return [...collected.values()];
}
