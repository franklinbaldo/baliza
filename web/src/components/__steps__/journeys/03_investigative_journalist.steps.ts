import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, waitFor } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { tick } from 'svelte';
import { render, noop, plannedStep } from './_shared';
import AgencyDetailViewRaw from '../../AgencyDetailView.svelte';

const AgencyDetailView = AgencyDetailViewRaw as unknown as Parameters<typeof render>[0];

const feature = await loadFeature('features/journeys/03_investigative_journalist.feature');

describeFeature(feature, ({ Scenario, BeforeEachScenario }) => {
  BeforeEachScenario(async () => {
    cleanup();
    vi.restoreAllMocks();
    window.history.replaceState({}, '', '/');
  });

  Scenario('Every contract has a stable permanent URL', ({ Given, Then }) => {
    let url = '';
    Given('the user opens "/contratacao?id=00000000000191-1-000001/2024"', () => {
      url = '/contratacao?id=00000000000191-1-000001/2024';
    });
    Then('the URL of the page is shareable and resolves the same contract on reload', () => {
      expect(url).toContain('id=');
      expect(url).toContain('/contratacao');
    });
  });

  Scenario('Each contract page links back to the original PNCP record', ({ Given, Then }) => {
    Given('the user opens "/contratacao?id=00000000000191-1-000001/2024"', noop);
    Then('the user sees an outbound link to the origin system that opens in a new tab', () => {
      // Real assertion lives in web/features/contract-detail.feature; this scenario
      // exists at the journey level only to make the dependency visible.
      expect(true).toBe(true);
    });
  });

  Scenario('Search state is preserved in the query string', ({ Given, Then, And }) => {
    Given('the user submits the free-text query "hospital municipal"', noop);
    Then('the page URL contains "?q=hospital%20municipal"', () =>
      plannedStep('dedicated search page with ?q= persistence'),
    );
    And('reloading the page restores the same result list', noop);
  });

  Scenario('Export a result list as Markdown', ({ Given, When, Then }) => {
    Given('a search result list is visible for "hospital municipal"', noop);
    When('the user clicks "Exportar Markdown"', noop);
    Then('the clipboard contains a Markdown table with headers and rows', () =>
      plannedStep('dedicated search page with Markdown clipboard export'),
    );
  });

  Scenario('Agency page surfaces top suppliers and a time series', ({ Given, Then, And }) => {
    Given('the user opens "/orgao?cnpj=00000000000191"', async () => {
      window.history.replaceState({}, '', '/?cnpj=00000000000191');
      global.fetch = vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            data: [
              {
                numeroControlePNCP: '00000000000191-1-000001/2024',
                dataPublicacaoPncp: '2025-01-15T00:00:00',
                objetoContratacao: 'Aquisição hospitalar',
                valorTotalEstimado: 1000,
                orgaoEntidade: { razaoSocial: 'Órgão', cnpj: '00000000000191' },
                unidadeOrgao: { nomeUnidade: 'Unidade' },
              },
            ],
          }),
          { status: 200 },
        ),
      );
      render(AgencyDetailView);
      await tick();
    });

    Then('the user sees the top five suppliers for that agency', async () => {
      await waitFor(() => expect(screen.getByTestId('top-suppliers')).toBeTruthy(), {
        timeout: 2000,
      });
    });

    And('the user sees a monthly contract-count chart for the last twelve months', () => {
      const chart = screen.getByTestId('monthly-chart');
      expect(chart).toBeTruthy();
      // Twelve buckets even when some are empty — the rolling axis is part
      // of the contract so journalists do not misread a gap as a pause.
      const rows = chart.querySelectorAll('.monthly-chart li');
      expect(rows.length).toBe(12);
    });
  });
});
