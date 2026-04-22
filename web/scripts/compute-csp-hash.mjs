import { readFileSync, readdirSync, statSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

/**
 * Walks web/dist/**\/*.html, extracts every inline <script> (no src attr),
 * computes sha256 base64, and prints:
 *   - a deduped set of `'sha256-...'` tokens ready to paste into script-src
 *   - per-hash sample of the first 120 chars of the hashed body
 *
 * Run after `npm run build`. Re-run whenever Layout.astro's inline script or
 * any other build-time inline script changes, and paste the resulting tokens
 * into the SCRIPT_SRC_HASHES array in SecurityHeaders.astro.
 */

const here = fileURLToPath(new URL('.', import.meta.url));
const distDir = join(here, '..', 'dist');

function walk(dir) {
  const out = [];
  for (const name of readdirSync(dir)) {
    const full = join(dir, name);
    const st = statSync(full);
    if (st.isDirectory()) out.push(...walk(full));
    else if (name.endsWith('.html')) out.push(full);
  }
  return out;
}

const INLINE_SCRIPT = /<script(?![^>]*\ssrc=)[^>]*>([\s\S]*?)<\/script>/g;

const hashes = new Map();
for (const file of walk(distDir)) {
  const html = readFileSync(file, 'utf8');
  let m;
  while ((m = INLINE_SCRIPT.exec(html)) !== null) {
    const body = m[1];
    if (body.trim() === '') continue;
    const hash = createHash('sha256').update(body, 'utf8').digest('base64');
    const token = `'sha256-${hash}'`;
    if (!hashes.has(token)) {
      hashes.set(token, {
        sample: body.slice(0, 120).replace(/\s+/g, ' ').trim(),
        firstSeenIn: relative(distDir, file),
      });
    }
  }
}

console.log('# CSP script-src hashes for inline scripts in dist/');
console.log(`# Found ${hashes.size} unique inline script(s).`);
console.log();
for (const [token, meta] of hashes) {
  console.log(token);
  console.log(`  first seen in: ${meta.firstSeenIn}`);
  console.log(`  sample: ${meta.sample}`);
  console.log();
}
console.log('# Paste the tokens above into SCRIPT_SRC_HASHES in');
console.log('# web/src/components/SecurityHeaders.astro.');
