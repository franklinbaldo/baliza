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

export function render(
  component: Parameters<typeof tlRender>[0],
  props?: Parameters<typeof tlRender>[1],
) {
  return tlRender(component as Parameters<typeof tlRender>[0], props);
}
