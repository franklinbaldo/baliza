import { describe, it, expect } from 'vitest';
import { readFileSync, readdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  CSP_DIRECTIVES,
  CONNECT_SRC,
  SCRIPT_SRC_HASHES,
  buildCsp,
} from './security-headers';

const SELF_FILE = 'security-headers.ts';

describe('buildCsp', () => {
  it('emits every declared directive exactly once', () => {
    const csp = buildCsp();
    for (const name of Object.keys(CSP_DIRECTIVES)) {
      const occurrences = csp.split('; ').filter((p) => p.startsWith(`${name} `));
      expect(occurrences, `directive ${name}`).toHaveLength(1);
    }
  });

  it('ends with upgrade-insecure-requests', () => {
    expect(buildCsp().split('; ').at(-1)).toBe('upgrade-insecure-requests');
  });

  it('forbids unsafe-eval and unsafe-inline in script-src', () => {
    const scriptSrc = CSP_DIRECTIVES['script-src'];
    expect(scriptSrc).not.toContain("'unsafe-eval'");
    expect(scriptSrc).not.toContain("'unsafe-inline'");
    // WASM instantiation needs wasm-unsafe-eval (DuckDB-wasm).
    expect(scriptSrc).toContain("'wasm-unsafe-eval'");
  });

  it('pins at least one script hash so inline scripts work without unsafe-inline', () => {
    expect(SCRIPT_SRC_HASHES.length).toBeGreaterThan(0);
    for (const h of SCRIPT_SRC_HASHES) {
      expect(h).toMatch(/^'sha256-[A-Za-z0-9+/=]+='$/);
    }
  });
});

describe('connect-src covers every external origin the client code contacts', () => {
  // Keep this test lightweight: scan every .ts/.svelte under src/lib/ plus the
  // components that do their own fetching, extract any https:// hostnames that
  // look like they are being contacted, and require each one to appear in the
  // CSP connect-src allow-list (directly or via a wildcard).
  // Walk the whole src/ tree — catches any fetch() that lands outside src/lib.
  const srcDir = join(dirname(fileURLToPath(import.meta.url)), '..');

  function walk(dir: string): string[] {
    const out: string[] = [];
    for (const name of readdirSync(dir, { withFileTypes: true })) {
      // Skip Cucumber/BDD step files — they carry example URLs that are not
      // contacted at runtime and would poison the allow-list check.
      if (name.isDirectory()) {
        if (name.name === '__steps__') continue;
        out.push(...walk(join(dir, name.name)));
        continue;
      }
      const full = join(dir, name.name);
      if (!name.isFile()) continue;
      if (!(name.name.endsWith('.ts') || name.name.endsWith('.svelte'))) continue;
      if (name.name.endsWith('.test.ts')) continue;
      // The policy file itself references every allow-listed host as string
      // literals — skipping it avoids circular self-checks (those hosts are
      // covered via style-src/font-src/script-src, not connect-src).
      if (name.name === SELF_FILE) continue;
      out.push(full);
    }
    return out;
  }

  const urlRe = /https:\/\/([a-z0-9.\-*]+)/gi;
  const foundHosts = new Set<string>();
  for (const file of walk(srcDir)) {
    const src = readFileSync(file, 'utf8');
    let m: RegExpExecArray | null;
    while ((m = urlRe.exec(src)) !== null) {
      foundHosts.add(m[1].toLowerCase());
    }
  }

  function isAllowed(host: string): boolean {
    for (const entry of CONNECT_SRC) {
      if (!entry.startsWith('https://')) continue;
      const allowed = entry.slice('https://'.length).toLowerCase();
      if (allowed === host) return true;
      if (allowed.startsWith('*.')) {
        const suffix = allowed.slice(1); // ".archive.org"
        if (host.endsWith(suffix)) return true;
      }
    }
    return false;
  }

  // Hosts that show up in comments/docstrings but aren't contacted at runtime.
  // Keep this list as small as possible — prefer updating CONNECT_SRC instead.
  const IGNORED_HOSTS = new Set<string>([
    'github.com',
    'franklinbaldo.github.io',
    'astro.build',
    'github.io',
  ]);

  for (const host of foundHosts) {
    if (IGNORED_HOSTS.has(host)) continue;
    it(`allows ${host} in connect-src`, () => {
      expect(
        isAllowed(host),
        `Host ${host} was referenced in src/lib/** but is not covered by CONNECT_SRC in security-headers.ts. ` +
          `Add it to CONNECT_SRC or, if it is only mentioned in documentation, to IGNORED_HOSTS in this test.`,
      ).toBe(true);
    });
  }
});

describe('style-src covers every external origin referenced from CSS', () => {
  // CSS @import / url() hits are subject to style-src, not connect-src. A
  // missing allow-list entry here makes Google Fonts (or any other web font
  // provider wired in via @import) silently fall back to system fonts in
  // production.
  const stylesDir = join(dirname(fileURLToPath(import.meta.url)), '..', 'styles');

  function findCssFiles(dir: string): string[] {
    const out: string[] = [];
    for (const name of readdirSync(dir, { withFileTypes: true })) {
      const full = join(dir, name.name);
      if (name.isDirectory()) out.push(...findCssFiles(full));
      else if (name.isFile() && name.name.endsWith('.css')) out.push(full);
    }
    return out;
  }

  const importRe = /@import\s+url\(\s*["']?(https:\/\/([a-z0-9.\-]+)[^"')]*)["']?\s*\)/gi;
  const cssHosts = new Set<string>();
  for (const file of findCssFiles(stylesDir)) {
    const src = readFileSync(file, 'utf8');
    let m: RegExpExecArray | null;
    while ((m = importRe.exec(src)) !== null) cssHosts.add(m[2].toLowerCase());
  }

  const styleSrc = CSP_DIRECTIVES['style-src'];
  for (const host of cssHosts) {
    it(`allows ${host} in style-src`, () => {
      expect(
        styleSrc.includes(`https://${host}`),
        `Host ${host} is @imported from a CSS file under src/styles/ but is not covered by style-src ` +
          `in security-headers.ts. Add https://${host} to style-src, and if the import chain references ` +
          `other hosts (e.g. font files), add them to font-src too.`,
      ).toBe(true);
    });
  }
});
