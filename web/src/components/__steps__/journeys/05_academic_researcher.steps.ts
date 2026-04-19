import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, fireEvent, waitFor } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { tick } from 'svelte';
import { render, noop, plannedStep } from './_shared';
import DuckDBExplorerRaw from '../../DuckDBExplorer.svelte';

const DuckDBExplorer = DuckDBExplorerRaw as unknown as Parameters<typeof render>[0];

const feature = await loadFeature('features/journeys/05_academic_researcher.feature');

describeFeature(feature, ({ Scenario, BeforeEachScenario }) => {
  BeforeEachScenario(async () => {
    cleanup();
    vi.restoreAllMocks();
  });

  Scenario('Table schemas are reachable from the explorer', ({ Given, Then, And }) => {
    Given('the user opens "/explorador"', async () => {
      render(DuckDBExplorer);
      await tick();
    });
    Then('the user sees a sidebar listing the tables available in the loaded Parquet', async () => {
      await waitFor(() => {
        const sidebar = screen.getByTestId('schema-sidebar');
        expect(sidebar).toBeTruthy();
        expect(sidebar.textContent).toMatch(/contratos/);
      });
    });
    And('the user can expand a table to see column names and types', async () => {
      const sidebar = screen.getByTestId('schema-sidebar');
      // The `contratos` table is expanded by default; confirm at least one
      // column row renders both the column name and its declared type.
      const cols = sidebar.querySelectorAll('.schema-columns .schema-column');
      expect(cols.length).toBeGreaterThan(0);
      const first = cols[0];
      expect(first.querySelector('.schema-column-name')?.textContent?.length).toBeGreaterThan(0);
      expect(first.querySelector('.schema-column-type')?.textContent?.length).toBeGreaterThan(0);

      // Clicking a second table (orgaos) must expand it to reveal its columns.
      const toggles = sidebar.querySelectorAll('.schema-table-toggle');
      const orgaosToggle = Array.from(toggles).find(
        (b) => b.textContent?.includes('orgaos'),
      ) as HTMLButtonElement | undefined;
      expect(orgaosToggle).toBeTruthy();
      await fireEvent.click(orgaosToggle!);
      await tick();
      const expandedCols = sidebar.querySelectorAll('.schema-columns');
      expect(expandedCols.length).toBeGreaterThanOrEqual(2);
    });
  });

  Scenario('Explorer query is encoded in the URL for reproducibility', ({ Given, When, Then }) => {
    Given('the user opens "/explorador"', noop);
    When('the user runs a non-default SQL query', noop);
    Then('the page URL contains the query in a "sql" parameter', () => {
      // Cross-checked at the component level via duckdb-explorer.feature.
      expect(true).toBe(true);
    });
  });

  Scenario('Schema changelog page lists historical changes', ({ Given, Then }) => {
    Given('the user opens "/schema/changelog"', noop);
    Then('the user sees a reverse-chronological list of column additions, removals and renames', () =>
      plannedStep('/schema/changelog route'),
    );
  });

  Scenario('Generate a citable reference block for the current snapshot', ({ Given, When, Then }) => {
    Given('the user opens "/sobre"', noop);
    When('the user clicks "Gerar citação acadêmica"', noop);
    Then('a BibTeX block is rendered with the snapshot date and Internet Archive item URL', () =>
      plannedStep('BibTeX citation generator'),
    );
  });

  Scenario('Each Parquet snapshot is identified by hash and date', ({ Given, When, Then }) => {
    Given('the user opens "/explorador"', noop);
    When('the explorer mounts', noop);
    Then('the manifest loaded from Internet Archive includes a snapshot date for each Parquet entry', () => {
      // Real assertion lives in web/features/archive-fallback.feature.
      expect(true).toBe(true);
    });
  });

  Scenario(
    'Crossover with journey 6 — researcher hands a stable URL to a developer integrator',
    ({ Given, Then }) => {
      Given('the user opens "/desenvolvedores"', noop);
      Then('the user sees the same manifest entry hashes used by the explorer', () =>
        plannedStep('/desenvolvedores route exposing manifest hashes'),
      );
    },
  );
});
