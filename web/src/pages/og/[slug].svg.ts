import type { APIRoute } from 'astro';
import { OG_ROUTES } from '../../lib/ogManifest';

export const prerender = true;

export function getStaticPaths() {
  return OG_ROUTES.map((route) => ({ params: { slug: route.slug } }));
}

function escapeXml(s: string): string {
  return s.replace(/[<>&'"]/g, (c) =>
    c === '<'
      ? '&lt;'
      : c === '>'
        ? '&gt;'
        : c === '&'
          ? '&amp;'
          : c === "'"
            ? '&apos;'
            : '&quot;',
  );
}

function wrap(text: string, max: number): string[] {
  const words = text.split(/\s+/);
  const lines: string[] = [];
  let current = '';
  for (const w of words) {
    if ((current + ' ' + w).trim().length > max) {
      if (current) lines.push(current);
      current = w;
    } else {
      current = (current ? current + ' ' : '') + w;
    }
  }
  if (current) lines.push(current);
  return lines.slice(0, 4);
}

export const GET: APIRoute = ({ params }) => {
  const slug = params.slug;
  const route = OG_ROUTES.find((r) => r.slug === slug);
  if (!route) {
    return new Response('Not found', { status: 404 });
  }

  const titleLines = wrap(route.title, 28);
  const subtitleLines = wrap(route.subtitle, 60);
  const accent = route.accent ?? '#f4b942';

  const titleSvg = titleLines
    .map((l, i) => `<tspan x="80" dy="${i === 0 ? 0 : 88}">${escapeXml(l)}</tspan>`)
    .join('');
  const subtitleSvg = subtitleLines
    .map((l, i) => `<tspan x="80" dy="${i === 0 ? 0 : 40}">${escapeXml(l)}</tspan>`)
    .join('');

  const titleY = 240;
  const subtitleY = titleY + 88 * (titleLines.length - 1) + 80;

  const svg = `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="630" viewBox="0 0 1200 630">
  <defs>
    <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0%" stop-color="#0d1117"/>
      <stop offset="100%" stop-color="#161b22"/>
    </linearGradient>
  </defs>
  <rect width="1200" height="630" fill="url(#bg)"/>
  <rect x="0" y="0" width="12" height="630" fill="${accent}"/>
  <text x="80" y="120" font-family="Inter, system-ui, sans-serif" font-size="32" fill="${accent}" font-weight="600" letter-spacing="6">BALIZA MONITOR</text>
  <text x="80" y="${titleY}" font-family="Inter, system-ui, sans-serif" font-size="76" fill="#f0f6fc" font-weight="700">${titleSvg}</text>
  <text x="80" y="${subtitleY}" font-family="Inter, system-ui, sans-serif" font-size="32" fill="#8b949e">${subtitleSvg}</text>
  <text x="80" y="570" font-family="Inter, system-ui, sans-serif" font-size="22" fill="#8b949e">Arquivo público das contratações nacionais · PNCP + Internet Archive</text>
</svg>`;

  return new Response(svg, {
    headers: {
      'Content-Type': 'image/svg+xml',
      'Cache-Control': 'public, max-age=86400',
    },
  });
};
