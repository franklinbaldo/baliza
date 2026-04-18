// PNCP's canonical `numeroControlePNCP` format: {cnpj}-{tipo}-{sequencial}/{ano}
// where cnpj is 14 digits, sequencial is 6 digits, and ano is 4 digits.
// Anything else is rejected at the boundary — no hyphen-only or short-year fallback.
const CANONICAL_PNCP_ID = /^(\d{14})-(\d+)-(\d{6})\/(\d{4})$/;

export interface PncpId {
  cnpj: string;
  tipo: string;
  sequencial: string;
  ano: string;
}

export function parsePncpId(raw: string): PncpId | null {
  const m = CANONICAL_PNCP_ID.exec(raw);
  if (!m) return null;
  return { cnpj: m[1], tipo: m[2], sequencial: m[3], ano: m[4] };
}

export function isPncpId(raw: string): boolean {
  return CANONICAL_PNCP_ID.test(raw);
}

export const PNCP_ID_EXAMPLE = '00000000000000-1-000001/2024';
