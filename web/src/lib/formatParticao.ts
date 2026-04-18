// The manifest CSV stores `data_particao` as a plain ISO date (YYYY-MM-DD).
// Detail views surface it inside the "dados arquivados" banner; render it in
// pt-BR long form (dd/MM/yyyy) so users see a locale-native string instead
// of the raw partition key. Unparseable or missing inputs fall back to a
// single em-dash so the banner never shows the literal "null" or partial
// junk.

const UNKNOWN = 'desconhecida';
const ISO_DATE = /^(\d{4})-(\d{2})-(\d{2})(?:[T ].*)?$/;

export function formatParticao(input: string | null | undefined): string {
  if (!input) return UNKNOWN;
  const match = ISO_DATE.exec(input);
  if (!match) return UNKNOWN;
  const [, y, m, d] = match;
  return `${d}/${m}/${y}`;
}
