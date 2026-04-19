// Shared display helpers. Every format fn returns an em-dash (`—`) on
// missing/invalid input so the UI never surfaces "null", "Invalid Date",
// or partial junk. `formatParticao` uses the pt-BR word "desconhecida"
// because it's rendered inline inside a full sentence banner.

const UNKNOWN_PARTICAO = 'desconhecida';
const EM_DASH = '—';
const ISO_DATE = /^(\d{4})-(\d{2})-(\d{2})(?:[T ].*)?$/;

// Zero-width space, non-joiner, joiner, word joiner, byte-order mark.
// Paste targets (URL bars, Word docs, WhatsApp forwards) routinely wrap
// identifiers in these or NBSP that String#trim does not remove. Strip
// them before running pattern detection so an ID like "12345678000195"
// with a trailing ZWSP still matches /^\d{14}$/.
const ZERO_WIDTH = /[\u200B-\u200D\u2060\uFEFF]/g;

export function formatBRL(v: number | null | undefined): string {
  if (v == null) return EM_DASH;
  return v.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
}

export function formatInteger(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return EM_DASH;
  return Math.trunc(v).toLocaleString('pt-BR');
}

// Buckets are deliberately coarse — freshness framing on the home doesn't
// need minute-accurate diffs, and we skip seconds to avoid "há 0 min" flicker.
// Future-dated inputs (clock skew, UTC vs local) are clamped to "agora"
// instead of falling through to a silently misleading relative string.
export function formatRelativeTime(
  iso: string | null | undefined,
  now: Date = new Date(),
): string {
  if (!iso) return EM_DASH;
  const then = new Date(iso);
  if (isNaN(then.getTime())) return EM_DASH;
  const diffMs = Math.max(0, now.getTime() - then.getTime());
  if (diffMs < 60_000) return 'agora';
  const minutes = Math.floor(diffMs / 60_000);
  if (minutes < 60) return `há ${minutes} min`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `há ${hours} h`;
  const days = Math.floor(hours / 24);
  return days === 1 ? 'há 1 dia' : `há ${days} dias`;
}

export function formatDate(iso: string | null | undefined): string {
  if (!iso) return EM_DASH;
  const d = new Date(iso);
  return isNaN(d.getTime()) ? EM_DASH : d.toLocaleDateString('pt-BR');
}

// The manifest CSV stores `data_particao` as a plain ISO date (YYYY-MM-DD).
// Detail views surface it inside the "dados arquivados" banner; render it
// in pt-BR long form (dd/MM/yyyy) so users see a locale-native string
// instead of the raw partition key.
export function formatParticao(input: string | null | undefined): string {
  if (!input) return UNKNOWN_PARTICAO;
  const match = ISO_DATE.exec(input);
  if (!match) return UNKNOWN_PARTICAO;
  const [, y, m, d] = match;
  return `${d}/${m}/${y}`;
}

export function normalizeSearchInput(raw: string): string {
  return raw.replace(ZERO_WIDTH, '').trim();
}
