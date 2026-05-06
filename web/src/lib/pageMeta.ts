/**
 * Runtime page-metadata sync.
 *
 * Detail views (contratacao, orgao, fornecedor, municipio, vigilancia) live
 * on a static Astro shell and resolve their record after mount. Build-time
 * OG cards therefore reflect the *route*, not the specific record. This
 * helper updates `<title>` and the og/twitter `<meta>` tags in-place once
 * data lands so:
 *
 *   - The browser tab and history entry show the record name.
 *   - In-app re-shares (via ShareButton → Web Share API) carry the right
 *     title/description into chat-app share-sheets.
 *
 * It does NOT help social-media crawlers, which fetch HTML without running
 * JS and will see only the build-time route-level OG card. Don't try to use
 * this for SEO previews — for that, generate per-record static pages.
 */
export function setPageMeta(meta: { title?: string; description?: string }): void {
  if (typeof document === 'undefined') return;

  if (meta.title) {
    document.title = meta.title.includes('Baliza') ? meta.title : `${meta.title} | Baliza Monitor`;
    setMeta('property', 'og:title', document.title);
    setMeta('property', 'twitter:title', document.title);
  }

  if (meta.description) {
    setMeta('name', 'description', meta.description);
    setMeta('property', 'og:description', meta.description);
    setMeta('property', 'twitter:description', meta.description);
  }
}

function setMeta(attr: 'name' | 'property', key: string, value: string): void {
  let el = document.querySelector(`meta[${attr}="${key}"]`) as HTMLMetaElement | null;
  if (!el) {
    el = document.createElement('meta');
    el.setAttribute(attr, key);
    document.head.appendChild(el);
  }
  el.setAttribute('content', value);
}
