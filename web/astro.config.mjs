import { defineConfig } from 'astro/config';
import svelte from '@astrojs/svelte';
import sitemap from '@astrojs/sitemap';

// https://astro.build/config
export default defineConfig({
  output: 'static',
  integrations: [svelte(), sitemap()],
  site: 'https://franklinbaldo.github.io',
  base: '/baliza',
  // Resolve 404 issues with/without slashes on GitHub Pages
  trailingSlash: 'ignore',
  vite: {
    optimizeDeps: {
      exclude: ['@duckdb/duckdb-wasm'],
      include: ['zod', '@tanstack/svelte-query'],
    },
    ssr: {
      noExternal: ['@tanstack/svelte-query', 'zod', 'lucide-svelte'],
    },
  },
});
