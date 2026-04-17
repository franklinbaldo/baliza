import { defineConfig } from 'vitest/config';
import { svelte } from '@sveltejs/vite-plugin-svelte';

export default defineConfig({
  plugins: [svelte({ hot: false })],
  test: {
    environment: 'jsdom',
    setupFiles: ['./vitest.setup.ts'],
    globals: true,
    include: [
      '**/*.{test,spec}.{js,mjs,cjs,ts,mts,cts}',
      '**/__steps__/**/*.steps.{js,ts}',
    ],
  },
  resolve: {
    conditions: ['browser'],
  },
});
