// Shared formatters for currency, dates, and similar surface text. Detail
// views, the search panel, and the archived banner all read from here so the
// presentation stays consistent and so component-local copies stop drifting.

export function formatBRL(value: number | null | undefined): string {
  if (value == null) return '—';
  return value.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
}

export function formatDate(
  iso: string | null | undefined,
  fallback = '—',
): string {
  if (!iso) return fallback;
  const d = new Date(iso);
  return isNaN(d.getTime()) ? fallback : d.toLocaleDateString('pt-BR');
}

export { formatParticao } from './formatParticao';
export { normalizeSearchInput } from './normalizeSearchInput';
