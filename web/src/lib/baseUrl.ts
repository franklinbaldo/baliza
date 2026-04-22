export const BASE = import.meta.env.BASE_URL.replace(/\/?$/, '/');

export function resolve(href: string): string {
  return `${BASE}${href.replace(/^\/+/, '')}`;
}
