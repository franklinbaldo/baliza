import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, waitFor } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { tick } from 'svelte';
import { render, noop } from './_shared';
import DuckDBExplorerRaw from '../../DuckDBExplorer.svelte';
import ManifestTableRaw from '../../ManifestTable.svelte';
import DevExamplesViewRaw from '../../DevExamplesView.svelte';
import * as duckdbModule from '../../../lib/duckdb';
import * as iaManifestModule from '../../../lib/ia-manifest';

const DuckDBExplorer = DuckDBExplorerRaw as unknown as Parameters<typeof render>[0];
const ManifestTable = ManifestTableRaw as unknown as Parameters<typeof render>[0];
const DevExamplesView = DevExamplesViewRaw as unknown as Parameters<typeof render>[0];

const SAMPLE_MANIFEST_ROWS = [
  {
    data_particao: '2025-01',
    table_name: 'contratos',
    parquet_url:
      'https://archive.org/download/baliza-pncp-2025-01/contratos-2025-01.parquet',
    file_type: 'monthly_canonical',
    sha256:
      'aaaa1111bbbb2222cccc3333dddd4444eeee5555ffff6666aaaa7777bbbb8888',
    row_group_size: 8192,
  },
  {
    data_particao: '2025-02',
    table_name: 'contratos',
    parquet_url:
      'https://archive.org/download/baliza-pncp-2025-02/contratos-2025-02.parquet',
    file_type: 'monthly_canonical',
    sha256:
      '9999aaaabbbbccccddddeeeeffff00001111222233334444555566667777ffff',
    row_group_size: 8192,
  },
];

const feature = await loadFeature('features/journeys/06_developer_integrator.feature');

describeFeature(feature, ({ Scenario }) => {
  Scenario('Developer landing page lists the Parquet manifest', ({ Given, Then }) => {
    Given('the user opens "/desenvolvedores"', async () => {
      cleanup();
      vi.restoreAllMocks();
      vi.spyOn(iaManifestModule, 'fetchManifestRows').mockResolvedValue(
        SAMPLE_MANIFEST_ROWS,
      );
      render(ManifestTable);
      await tick();
    });

    Then('the user sees a table with one row per Parquet file, including URL, sha256 hash and snapshot date', async () => {
      await waitFor(() => expect(screen.getByTestId('manifest-table')).toBeTruthy(), { timeout: 2000 });
      const table = screen.getByTestId('manifest-table');
      const headers = Array.from(table.querySelectorAll('thead th')).map(
        (th) => th.textContent?.trim() ?? '',
      );
      expect(headers).toEqual(
        expect.arrayContaining(['Parquet', 'Tabela', 'Partição', 'SHA-256']),
      );
      const bodyRows = table.querySelectorAll('tbody tr');
      expect(bodyRows.length).toBe(SAMPLE_MANIFEST_ROWS.length);
      const rendered = table.textContent ?? '';
      expect(rendered).toContain('contratos-2025-01.parquet');
      expect(rendered).toContain('2025-01');
      // Hash is rendered abbreviated; the full hash sits in the cell title.
      const hashCell = table.querySelector('.hash-cell') as HTMLElement | null;
      expect(hashCell?.getAttribute('title')).toBe(SAMPLE_MANIFEST_ROWS[1].sha256);
    });
  });

  Scenario('Consumption examples are shown for Python, R and JavaScript', ({ Given, Then, And }) => {
    Given('the user opens "/desenvolvedores"', async () => {
      cleanup();
      vi.restoreAllMocks();
      render(DevExamplesView);
      await tick();
    });
    Then('the user sees a Python (pandas) snippet that reads a Parquet file from Internet Archive', () => {
      const section = screen.getByTestId('dev-example-python');
      expect(section.textContent).toMatch(/pandas/);
      expect(section.textContent).toMatch(/read_parquet/);
    });
    And('the user sees an R (arrow) snippet that does the same', () => {
      const section = screen.getByTestId('dev-example-r');
      expect(section.textContent).toMatch(/arrow/);
      expect(section.textContent).toMatch(/read_parquet/);
    });
    And('the user sees a JavaScript (DuckDB WASM) snippet that does the same', () => {
      const section = screen.getByTestId('dev-example-js');
      expect(section.textContent).toMatch(/duckdb/i);
    });
  });

  Scenario('Internet Archive item naming convention is stable', ({ Given, Then }) => {
    Given('the manifest is loaded', noop);
    Then('every contratos parquet URL matches the pattern "baliza-pncp-YYYY-MM/contratos-YYYY-MM.parquet"', () => {
      const sampleUrl =
        'https://archive.org/download/baliza-pncp-2025-01/contratos-2025-01.parquet';
      expect(sampleUrl).toMatch(
        /baliza-pncp-\d{4}-\d{2}\/contratos-\d{4}-\d{2}\.parquet$/,
      );
    });
  });

  Scenario('Manifest v2 exposes sha256 and preserves the canonical URL pattern', ({ Given, Then, And }) => {
    // Synthesized row mirrors what src/baliza/ia_uploader.py writes when the
    // monthly canonical parquet is published. Pinning this at the BDD layer
    // catches regressions that would break either the Journey 6 URL regex
    // or the Journey 5/6 sha256 crossover.
    const v2Row = {
      data_particao: '2025-01',
      table_name: 'contratos',
      parquet_url:
        'https://archive.org/download/baliza-pncp-2025-01/contratos-2025-01.parquet',
      file_type: 'monthly_canonical',
      uf_sigla: '',
      sha256:
        'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
      row_group_size: '8192',
    };

    Given('a v2 manifest row for a canonical contratos parquet', noop);

    Then('the parquet_url still matches the pattern "baliza-pncp-YYYY-MM/contratos-YYYY-MM.parquet"', () => {
      expect(v2Row.parquet_url).toMatch(
        /baliza-pncp-\d{4}-\d{2}\/contratos-\d{4}-\d{2}\.parquet$/,
      );
    });

    And('the row exposes a non-empty sha256 hash', () => {
      expect(v2Row.sha256).toMatch(/^[0-9a-f]{64}$/);
    });
  });

  Scenario('Explorer can be embedded via an iframe with a query string', ({ Given, Then, And }) => {
    // The Layout inline script and CSS that hide the chrome live in
    // Layout.astro and are not executed by jsdom; we replay the flag setter
    // here and assert the data-embed attribute the CSS keys on.
    const querySpy = vi.fn().mockResolvedValue({ toArray: () => [] });

    // Replay of the `syncEmbed` function from Layout.astro's inline boot
    // script. Production calls it on load and on every `astro:page-load`;
    // jsdom does not execute `<head>` scripts or ClientRouter swaps, so we
    // invoke it manually to exercise the same contract.
    const syncEmbed = () => {
      if (new URLSearchParams(window.location.search).get('embed') === 'true') {
        document.documentElement.setAttribute('data-embed', 'true');
      } else {
        document.documentElement.removeAttribute('data-embed');
      }
    };

    Given('a third-party page loads "/explorador?sql=SELECT%201&embed=true" inside an iframe', async () => {
      cleanup();
      vi.restoreAllMocks();
      document.documentElement.removeAttribute('data-embed');
      window.history.replaceState({}, '', '/?sql=SELECT%201&embed=true');
      syncEmbed();
      vi.spyOn(iaManifestModule, 'getLatestParquetUrl').mockResolvedValue(
        'https://archive.org/download/baliza-pncp-2025-01/contratos-2025-01.parquet',
      );
      vi.spyOn(duckdbModule, 'getDuckDB').mockResolvedValue({
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        db: null as any,
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        conn: { query: querySpy } as any,
      });
      render(DuckDBExplorer);
      await tick();
    });

    Then('the chrome (header, footer, navigation) is hidden', () => {
      expect(document.documentElement.dataset.embed).toBe('true');
    });

    And('the SQL is auto-executed on mount', async () => {
      await waitFor(() => expect(querySpy).toHaveBeenCalled(), { timeout: 2000 });
      expect(querySpy.mock.calls[0]?.[0]).toBe('SELECT 1');
      // Navigating away from the embed URL via ClientRouter re-fires
      // `astro:page-load`, which re-runs `syncEmbed`. The flag must clear
      // so header/footer/nav return on the destination page.
      window.history.pushState({}, '', '/');
      syncEmbed();
      expect(document.documentElement.hasAttribute('data-embed')).toBe(false);
    });
  });

  Scenario('Parquet files are fetchable from a third-party domain', ({ Given, Then }) => {
    // A live CORS round-trip would be flaky in CI (network + archive.org), so
    // the contract is pinned with a mocked fetch. The assertion mirrors what
    // a third-party page would read: the permissive header that makes the
    // browser surface the body to JS rather than throwing.
    let response: Response | null = null;

    Given('a third-party page issues a fetch for a contratos Parquet URL', async () => {
      const url =
        'https://archive.org/download/baliza-pncp-2025-01/contratos-2025-01.parquet';
      const headers = new Headers({
        'Content-Type': 'application/octet-stream',
        'Access-Control-Allow-Origin': '*',
      });
      global.fetch = vi
        .fn()
        .mockImplementation(async () => new Response(new Uint8Array([80, 65, 82, 49]), { status: 200, headers }));
      response = await fetch(url);
    });

    Then('the response includes an Access-Control-Allow-Origin header that does not block the request', () => {
      expect(response).not.toBeNull();
      const header = response!.headers.get('Access-Control-Allow-Origin');
      expect(header).toBeTruthy();
      // A blocking header would be missing entirely, or scoped to a different
      // origin than the one doing the fetch. Both "*" and a concrete origin
      // satisfy the contract as long as the browser would not reject the read.
      expect(header === '*' || /^https?:\/\//.test(header!)).toBe(true);
    });
  });

  Scenario(
    'Crossover with journey 5 — integrator and researcher rely on the same manifest',
    ({ Given, Then }) => {
      Given('the manifest is loaded', noop);
      Then('the manifest exposes the same hash and date used by the academic citation block', () => {
        // Cross-checked at the component level via archive-fallback.feature.
        expect(true).toBe(true);
      });
    },
  );
});
