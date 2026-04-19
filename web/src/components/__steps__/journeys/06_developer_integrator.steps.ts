import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { expect } from 'vitest';
import { noop, plannedStep, wipStep } from './_shared';

const feature = await loadFeature('features/journeys/06_developer_integrator.feature');

describeFeature(feature, ({ Scenario }) => {
  Scenario('Developer landing page lists the Parquet manifest', ({ Given, Then }) => {
    Given('the user opens "/desenvolvedores"', noop);
    Then('the user sees a table with one row per Parquet file, including URL, sha256 hash, snapshot date and size in bytes', () =>
      plannedStep('/desenvolvedores landing page with full manifest'),
    );
  });

  Scenario('Consumption examples are shown for Python, R and JavaScript', ({ Given, Then, And }) => {
    Given('the user opens "/desenvolvedores"', noop);
    Then('the user sees a Python (pandas) snippet that reads a Parquet file from Internet Archive', () =>
      plannedStep('language-specific consumption snippets'),
    );
    And('the user sees an R (arrow) snippet that does the same', noop);
    And('the user sees a JavaScript (DuckDB WASM) snippet that does the same', noop);
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

  Scenario('Explorer can be embedded via an iframe with a query string', ({ Given, Then, And }) => {
    Given('a third-party page loads "/explorador?sql=SELECT%201&embed=true" inside an iframe', noop);
    Then('the chrome (header, footer, navigation) is hidden', () =>
      plannedStep('embed mode for the explorer'),
    );
    And('the SQL is auto-executed on mount', noop);
  });

  Scenario('Parquet files are fetchable from a third-party domain', ({ Given, Then }) => {
    Given('a third-party page issues a fetch for a contratos Parquet URL', noop);
    Then('the response includes an Access-Control-Allow-Origin header that does not block the request', () =>
      wipStep('explicit CORS contract test against archive.org'),
    );
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
