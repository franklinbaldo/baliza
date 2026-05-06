import { describe, it, expect, beforeEach } from 'vitest';
import { replaceUrlParams } from './urlState';

describe('replaceUrlParams', () => {
  beforeEach(() => {
    window.history.replaceState({}, '', '/');
  });

  it('writes a single param, keeping the pathname', () => {
    replaceUrlParams({ q: 'merenda' });
    expect(window.location.search).toBe('?q=merenda');
  });

  it('drops empty/undefined values', () => {
    replaceUrlParams({ q: 'merenda', uf: '', mod: undefined });
    expect(window.location.search).toBe('?q=merenda');
  });

  it('merges with existing params by default', () => {
    window.history.replaceState({}, '', '/?cnpj=12345');
    replaceUrlParams({ dias: 30 });
    expect(window.location.search).toContain('cnpj=12345');
    expect(window.location.search).toContain('dias=30');
  });

  it('replaces all params when merge=false', () => {
    window.history.replaceState({}, '', '/?cnpj=12345&old=value');
    replaceUrlParams({ q: 'novo' }, { merge: false });
    expect(window.location.search).toBe('?q=novo');
  });

  it('removes a key by passing empty string under merge', () => {
    window.history.replaceState({}, '', '/?q=foo&dias=30');
    replaceUrlParams({ dias: '' });
    expect(window.location.search).toBe('?q=foo');
  });

  it('omits the query string when no params remain', () => {
    replaceUrlParams({ q: '' });
    expect(window.location.search).toBe('');
    expect(window.location.pathname).toBe('/');
  });
});
