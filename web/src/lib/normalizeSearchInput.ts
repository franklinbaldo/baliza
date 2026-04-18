// Paste targets (URL bars, Word docs, WhatsApp forwards) routinely wrap
// identifiers in zero-width characters or NBSP that String#trim does not
// remove. Strip those before running pattern detection so an ID like
// "12345678000195" with a trailing ZWSP still matches /^\d{14}$/.

// Zero-width space, non-joiner, joiner, word joiner, byte-order mark.
const ZERO_WIDTH = /[\u200B-\u200D\u2060\uFEFF]/g;

export function normalizeSearchInput(raw: string): string {
  return raw.replace(ZERO_WIDTH, '').trim();
}
