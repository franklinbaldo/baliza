import { describe, it, expect } from 'vitest';
import { formatParticao } from './formatParticao';

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
