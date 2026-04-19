import { describe, it, expect } from 'vitest';
import {
  formatBRL,
  formatDate,
  formatParticao,
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
    expect(formatDate('2024-05-01')).toBe('01/05/2024');
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
