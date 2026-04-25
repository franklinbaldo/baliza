// Thin wrapper around window.location.assign so tests can spy via vi.spyOn
// without fighting jsdom, where Location.assign is non-configurable and
// defineProperty refuses to redefine it even with configurable: true.
export function navigate(url: string): void {
  if (typeof window !== 'undefined') {
    window.location.assign(url);
  }
}

// Like navigate() but replaces the current history entry (no new entry added).
// Use when the current page should not be reachable via the Back button.
export function navigateReplace(url: string): void {
  if (typeof window !== 'undefined') {
    window.location.replace(url);
  }
}
