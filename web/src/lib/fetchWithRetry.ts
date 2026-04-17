export async function fetchWithRetry(
  url: string,
  options: RequestInit = {},
  maxRetries = 3,
): Promise<Response> {
  if (typeof window === 'undefined') {
    return fetch(url, options);
  }

  let lastError: Error | null = null;

  for (let i = 0; i < maxRetries; i++) {
    try {
      const response = await fetch(url, options);
      if (!response.ok && response.status >= 500) {
        throw new Error(`HTTP ${response.status}`);
      }
      window.dispatchEvent(new CustomEvent('baliza-network-success'));
      return response;
    } catch (err) {
      lastError = err as Error;
      if (i < maxRetries - 1) {
        const delay = Math.min(1000 * Math.pow(2, i), 30_000);
        window.dispatchEvent(
          new CustomEvent('baliza-network-retry', {
            detail: { attempt: i + 1, maxRetries, delay },
          }),
        );
        await new Promise((r) => setTimeout(r, delay));
      }
    }
  }

  window.dispatchEvent(new CustomEvent('baliza-network-error'));
  throw lastError ?? new Error('Fetch failed');
}
