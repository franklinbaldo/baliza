// Thin wrapper around PNCP's /v1/contratacoes/publicacao list endpoint.
//
// The endpoint requires `dataInicial`, `dataFinal`, `codigoModalidadeContratacao`,
// and `pagina` on every call, and only accepts a single modality per request.
// To get a realistic "recent contracts" list for a município or órgão we have
// to fan out across the common modalities and merge the results. The callers
// used to omit every required parameter and silently 400'd — see PR #355.

import { parsePncpPublicacaoList, type PNCPContract } from './pncp';

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
  tamanhoPagina?: number;
  modalidades?: readonly number[];
  /** Injectable for tests / SSR. Defaults to `new Date()`. */
  now?: Date;
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
  dataInicial: string,
  dataFinal: string,
  tamanhoPagina: number,
): string {
  const params = new URLSearchParams({
    dataInicial,
    dataFinal,
    codigoModalidadeContratacao: String(modalidade),
    pagina: '1',
    tamanhoPagina: String(tamanhoPagina),
  });
  if (filters.cnpj) params.set('cnpj', filters.cnpj);
  if (filters.codigoMunicipioIbge) params.set('codigoMunicipioIbge', filters.codigoMunicipioIbge);
  return `${PUBLICACAO_URL}?${params.toString()}`;
}

// Fan out across modalities with Promise.allSettled so one 500 doesn't kill the
// page. Merge, dedupe by numeroControlePNCP (the canonical PNCP identity),
// sort by dataPublicacaoPncp DESC, slice to tamanhoPagina. If every modality
// rejects, rethrow so the caller's archive fallback kicks in.
export async function fetchPublicacaoList(
  filters: PublicacaoFilters,
  opts: PublicacaoOpts = {},
): Promise<PNCPContract[]> {
  const { sinceDays = DEFAULT_SINCE_DAYS, tamanhoPagina = 10, modalidades = DEFAULT_MODALIDADES, now = new Date() } = opts;
  const clamped = Math.max(10, Math.min(50, tamanhoPagina));
  const end = new Date(now);
  const start = new Date(now);
  start.setUTCDate(start.getUTCDate() - sinceDays);
  const dataInicial = yyyymmdd(start);
  const dataFinal = yyyymmdd(end);

  const settled = await Promise.allSettled(
    modalidades.map(async (modalidade) => {
      const url = buildUrl(filters, modalidade, dataInicial, dataFinal, clamped);
      const res = await fetch(url);
      if (!res.ok) throw new Error(`PNCP publicacao returned ${res.status} for modalidade ${modalidade}`);
      return parsePncpPublicacaoList(await res.json());
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
