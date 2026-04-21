import { describe, it, expect } from 'vitest';
import { resolveInitialCity, DEFAULT_CITY } from './cityContext.svelte';

function storage(initial: Record<string, string> = {}): Storage {
  const data = new Map(Object.entries(initial));
  return {
    get length() {
      return data.size;
    },
    clear: () => data.clear(),
    getItem: (k: string) => data.get(k) ?? null,
    key: (i: number) => Array.from(data.keys())[i] ?? null,
    removeItem: (k: string) => void data.delete(k),
    setItem: (k: string, v: string) => void data.set(k, String(v)),
  };
}

describe('resolveInitialCity', () => {
  it('falls back to Porto Velho, RO when nothing is set', () => {
    const got = resolveInitialCity();
    expect(got).toEqual({ ...DEFAULT_CITY, source: 'default' });
    expect(got.ibge).toBe('1100205');
    expect(got.uf).toBe('RO');
  });

  it('honors the URL param over storage and default', () => {
    const got = resolveInitialCity({
      urlParams: new URLSearchParams('?ibge=3550308&uf=SP&cidade=S%C3%A3o%20Paulo'),
      storage: storage({
        'baliza-city': JSON.stringify({ ibge: '3304557', nome: 'Rio', uf: 'RJ' }),
      }),
    });
    expect(got).toEqual({
      ibge: '3550308',
      nome: 'São Paulo',
      uf: 'SP',
      source: 'url',
    });
  });

  it('uses localStorage when URL has no ibge', () => {
    const got = resolveInitialCity({
      urlParams: new URLSearchParams(''),
      storage: storage({
        'baliza-city': JSON.stringify({ ibge: '3304557', nome: 'Rio de Janeiro', uf: 'RJ' }),
      }),
    });
    expect(got.ibge).toBe('3304557');
    expect(got.source).toBe('storage');
  });

  it('ignores a malformed ibge in the URL', () => {
    const got = resolveInitialCity({
      urlParams: new URLSearchParams('?ibge=nope'),
      storage: storage(),
    });
    expect(got.source).toBe('default');
    expect(got.ibge).toBe(DEFAULT_CITY.ibge);
  });

  it('ignores malformed localStorage JSON', () => {
    const got = resolveInitialCity({
      urlParams: new URLSearchParams(''),
      storage: storage({ 'baliza-city': '{not json' }),
    });
    expect(got.source).toBe('default');
  });

  it('drops an invalid UF but keeps the ibge', () => {
    const got = resolveInitialCity({
      urlParams: new URLSearchParams('?ibge=3550308&uf=zzz'),
      storage: storage(),
    });
    expect(got.ibge).toBe('3550308');
    expect(got.uf).toBe('');
    expect(got.source).toBe('url');
  });
});
