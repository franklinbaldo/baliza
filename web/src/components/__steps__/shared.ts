import { vi } from 'vitest';
import { render as tlRender } from '@testing-library/svelte/pure';
import { QueryClient } from '@tanstack/svelte-query';

vi.mock('../../lib/queryClient', () => ({
  getQueryClient: () =>
    new QueryClient({
      defaultOptions: { queries: { retry: false, staleTime: 0 } },
    }),
}));

vi.mock('../../lib/duckdb', () => ({
  getDuckDB: vi.fn().mockResolvedValue({
    db: null,
    conn: {
      query: vi.fn().mockResolvedValue({ toArray: () => [] }),
    },
  }),
}));

vi.mock('../../lib/ia-manifest', () => ({
  getLatestParquetUrl: vi.fn().mockResolvedValue(null),
  getLatestParquetInfo: vi.fn().mockResolvedValue(null),
  fetchManifestRows: vi.fn().mockResolvedValue([]),
  resetIaManifestCache: vi.fn(),
  IA_MANIFEST_URL: 'https://archive.org/download/baliza-pncp-manifest/manifest.csv',
}));

vi.mock('../../lib/parquetFallback', async () => {
  const actual = await vi.importActual<typeof import('../../lib/parquetFallback')>(
    '../../lib/parquetFallback',
  );
  return {
    ...actual,
    queryParquetFallback: vi.fn().mockResolvedValue({ ok: false, reason: 'empty' }),
  };
});

export function render(
  component: Parameters<typeof tlRender>[0],
  props?: Parameters<typeof tlRender>[1],
) {
  return tlRender(component as Parameters<typeof tlRender>[0], props);
}
