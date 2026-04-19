import { describe, it, expect } from 'vitest';
import { formatBRL, formatDate } from './format';

describe('formatBRL', () => {
  it('formats a number as BRL currency in pt-BR', () => {
    expect(formatBRL(1234.56)).toMatch(/R\$\s?1\.234,56/);
  });

  it('returns the dash placeholder for null', () => {
    expect(formatBRL(null)).toBe('—');
  });

  it('returns the dash placeholder for undefined', () => {
    expect(formatBRL(undefined)).toBe('—');
  });
});

describe('formatDate', () => {
  it('formats an ISO date string as dd/mm/yyyy', () => {
    expect(formatDate('2024-05-01T00:00:00')).toBe('01/05/2024');
  });

  it('returns the default fallback for missing input', () => {
    expect(formatDate(null)).toBe('—');
    expect(formatDate(undefined)).toBe('—');
    expect(formatDate('')).toBe('—');
  });

  it('honours a custom fallback when input is missing', () => {
    expect(formatDate(undefined, '')).toBe('');
  });

  it('returns the fallback for unparseable dates', () => {
    expect(formatDate('not-a-date')).toBe('—');
    expect(formatDate('not-a-date', '')).toBe('');
  });
});
