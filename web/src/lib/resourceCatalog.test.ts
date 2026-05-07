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

  it('marks contratos, atas, publicacoes and pca as registered', () => {
    for (const name of ['contratos', 'atas', 'publicacoes', 'pca']) {
      expect(REGISTERED_RESOURCE_NAMES.has(name)).toBe(true);
      expect(getResourceEntry(name)?.status).toBe('registered');
    }
  });

  it('marks itens as planned (not yet queryable)', () => {
    expect(REGISTERED_RESOURCE_NAMES.has('itens')).toBe(false);
    expect(getResourceEntry('itens')?.status).toBe('planned');
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
