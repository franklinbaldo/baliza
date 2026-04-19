import { describe, it, expect } from 'vitest';
import {
  formatBRL,
  formatDate,
  formatInteger,
  formatParticao,
  formatRelativeTime,
  normalizeSearchInput,
} from './format';

describe('formatBRL', () => {
  it('renders pt-BR currency with decimals', () => {
    expect(formatBRL(1000)).toBe('R$\u00a01.000,00');
  });

  it('returns em-dash for null', () => {
    expect(formatBRL(null)).toBe('—');
  });

  it('returns em-dash for undefined', () => {
    expect(formatBRL(undefined)).toBe('—');
  });
});

describe('formatDate', () => {
  it('renders ISO dates as dd/MM/yyyy', () => {
    // Use a datetime with explicit local-time hour so the assertion holds in
    // any TZ. A bare "2024-05-01" parses as UTC midnight and shifts one day
    // back under negative offsets (e.g. America/Los_Angeles).
    expect(formatDate('2024-05-01T12:00:00')).toBe('01/05/2024');
  });

  it('returns em-dash for null', () => {
    expect(formatDate(null)).toBe('—');
  });

  it('returns em-dash for undefined', () => {
    expect(formatDate(undefined)).toBe('—');
  });

  it('returns em-dash for unparseable garbage', () => {
    expect(formatDate('not-a-date')).toBe('—');
  });
});

describe('formatParticao', () => {
  it('renders ISO dates as dd/MM/yyyy', () => {
    expect(formatParticao('2024-12-01')).toBe('01/12/2024');
  });

  it('accepts ISO datetimes and keeps the dd/MM/yyyy form', () => {
    expect(formatParticao('2024-12-01T00:00:00')).toBe('01/12/2024');
  });

  it('returns "desconhecida" when the value is null', () => {
    expect(formatParticao(null)).toBe('desconhecida');
  });

  it('returns "desconhecida" when the value is undefined', () => {
    expect(formatParticao(undefined)).toBe('desconhecida');
  });

  it('returns "desconhecida" for unparseable garbage', () => {
    expect(formatParticao('not-a-date')).toBe('desconhecida');
  });
});

describe('formatInteger', () => {
  it('adds pt-BR thousand separator', () => {
    expect(formatInteger(5804)).toBe('5.804');
  });

  it('formats six-figure numbers', () => {
    expect(formatInteger(1234567)).toBe('1.234.567');
  });

  it('leaves sub-thousand values alone', () => {
    expect(formatInteger(42)).toBe('42');
  });

  it('truncates decimals (stat counters are integers)', () => {
    expect(formatInteger(1500.9)).toBe('1.500');
  });

  it('returns em-dash for null', () => {
    expect(formatInteger(null)).toBe('—');
  });

  it('returns em-dash for undefined', () => {
    expect(formatInteger(undefined)).toBe('—');
  });

  it('returns em-dash for NaN', () => {
    expect(formatInteger(Number.NaN)).toBe('—');
  });
});

describe('formatRelativeTime', () => {
  const now = new Date('2026-04-19T12:00:00Z');

  it('says "agora" for diffs under a minute', () => {
    expect(formatRelativeTime('2026-04-19T11:59:30Z', now)).toBe('agora');
  });

  it('bucketed minutes', () => {
    expect(formatRelativeTime('2026-04-19T11:48:00Z', now)).toBe('há 12 min');
  });

  it('bucketed hours', () => {
    expect(formatRelativeTime('2026-04-19T08:00:00Z', now)).toBe('há 4 h');
  });

  it('singular day', () => {
    expect(formatRelativeTime('2026-04-18T12:00:00Z', now)).toBe('há 1 dia');
  });

  it('plural days', () => {
    expect(formatRelativeTime('2026-04-17T12:00:00Z', now)).toBe('há 2 dias');
  });

  it('clamps future timestamps to "agora" (clock skew guard)', () => {
    expect(formatRelativeTime('2026-04-19T15:00:00Z', now)).toBe('agora');
  });

  it('returns em-dash for null', () => {
    expect(formatRelativeTime(null, now)).toBe('—');
  });

  it('returns em-dash for unparseable garbage', () => {
    expect(formatRelativeTime('not-a-date', now)).toBe('—');
  });
});

describe('normalizeSearchInput', () => {
  it('trims leading and trailing whitespace', () => {
    expect(normalizeSearchInput('  hello  ')).toBe('hello');
  });

  it('strips a trailing zero-width space', () => {
    expect(normalizeSearchInput('12345678000195\u200B')).toBe('12345678000195');
  });

  it('strips embedded zero-width joiners and non-joiners', () => {
    expect(normalizeSearchInput('abc\u200Cdef\u200Dghi')).toBe('abcdefghi');
  });

  it('strips a leading BOM', () => {
    expect(normalizeSearchInput('\uFEFFhello')).toBe('hello');
  });

  it('preserves interior spaces when stripping zero-width', () => {
    expect(normalizeSearchInput('a\u200B b\u200D c')).toBe('a b c');
  });
});
