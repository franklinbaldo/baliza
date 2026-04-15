import { QueryClient } from '@tanstack/svelte-query';

let queryClient: QueryClient | undefined;

/**
 * Singleton pattern for QueryClient to ensure consistent cache 
 * across all island components.
 */
export function getQueryClient() {
  if (!queryClient) {
    queryClient = new QueryClient({
      defaultOptions: {
        queries: {
          staleTime: 60 * 1000, // 1 minute
          retry: 1,
          refetchOnWindowFocus: false,
        },
      },
    });
  }
  return queryClient;
}
