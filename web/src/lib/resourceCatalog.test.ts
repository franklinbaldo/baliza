import { describe, it, expect } from 'vitest';
import {
  RESOURCE_CATALOG,
  REGISTERED_RESOURCE_NAMES,
  getResourceEntry,
} from './resourceCatalog';

describe('resourceCatalog', () => {
  it('covers the conceptual spine PCA → Publicações → Atas → Contratos → Itens', () => {
    expect(RESOURCE_CATALOG.map((r) => r.name)).toEqual([
      'pca',
      'publicacoes',
      'atas',
      'contratos',
      'itens',
    ]);
  });

  it('marks contratos and atas as registered today', () => {
    expect(REGISTERED_RESOURCE_NAMES.has('contratos')).toBe(true);
    expect(REGISTERED_RESOURCE_NAMES.has('atas')).toBe(true);
  });

  it('marks publicacoes, pca and itens as planned (not yet queryable)', () => {
    for (const name of ['publicacoes', 'pca', 'itens']) {
      expect(REGISTERED_RESOURCE_NAMES.has(name)).toBe(false);
      expect(getResourceEntry(name)?.status).toBe('planned');
    }
  });

  it('exposes a label and a description for every entry', () => {
    for (const r of RESOURCE_CATALOG) {
      expect(r.label.length).toBeGreaterThan(0);
      expect(r.description.length).toBeGreaterThan(0);
    }
  });

  it('returns undefined for an unknown resource name', () => {
    expect(getResourceEntry('not-a-resource')).toBeUndefined();
  });
});
