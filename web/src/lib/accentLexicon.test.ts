import { describe, it, expect } from 'vitest';
import { stripAccents, suggestAccented } from './accentLexicon';

describe('stripAccents', () => {
  it('removes diacritics from common Portuguese words', () => {
    expect(stripAccents('construção')).toBe('construcao');
    expect(stripAccents('São Paulo')).toBe('Sao Paulo');
    expect(stripAccents('aquisição de veículos')).toBe('aquisicao de veiculos');
  });

  it('is a no-op on already-unaccented strings', () => {
    expect(stripAccents('hospital municipal')).toBe('hospital municipal');
  });
});

describe('suggestAccented', () => {
  it('suggests the canonical accented form for an unaccented word', () => {
    expect(suggestAccented('construcao de escola')).toBe('construção de escola');
    expect(suggestAccented('aquisicao de veiculos')).toBe('aquisição de veículos');
  });

  it('returns null when the term already matches canonical accents', () => {
    expect(suggestAccented('construção de escola')).toBeNull();
  });

  it('returns null when no word is in the lexicon', () => {
    expect(suggestAccented('hospital municipal')).toBeNull();
  });

  it('returns null on empty input', () => {
    expect(suggestAccented('')).toBeNull();
    expect(suggestAccented('   ')).toBeNull();
  });

  it('preserves whitespace between tokens', () => {
    expect(suggestAccented('construcao  de  escola')).toBe('construção  de  escola');
  });
});
