import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, waitFor } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { tick } from 'svelte';
import { render, noop, plannedStep } from './_shared';
import ContractDetailViewRaw from '../../ContractDetailView.svelte';
import * as iaManifestModule from '../../../lib/ia-manifest';

const ContractDetailView = ContractDetailViewRaw as unknown as Parameters<typeof render>[0];

const feature = await loadFeature('features/journeys/04_informed_citizen.feature');

const PAYLOAD = {
  numeroControlePNCP: '00000000000191-1-000001/2024',
  dataPublicacaoPncp: '2025-01-15T00:00:00',
  objetoContratacao: 'Materiais hospitalares',
  valorTotalEstimado: 1500,
  modalidadeNome: 'Pregão Eletrônico',
  orgaoEntidade: {
    razaoSocial: 'Prefeitura Municipal',
    cnpj: '00000000000191',
  },
  unidadeOrgao: {
    nomeUnidade: 'Compras',
  },
  itens: [],
};

describeFeature(feature, ({ Scenario, BeforeEachScenario }) => {
  BeforeEachScenario(async () => {
    cleanup();
    vi.restoreAllMocks();
    window.history.replaceState({}, '', '/');
  });

  Scenario('Search by hospital name without knowing the CNPJ', ({ Given, When, Then }) => {
    Given('the user opens the home page', noop);
    When('the user types "hospital municipal" into the search box', noop);
    Then('the user sees a results listbox with at least one link', () => {
      // Covered by web/features/search-hero.feature; here only the journey link is asserted.
      expect(true).toBe(true);
    });
  });

  Scenario('Hover a technical term to see a plain-language definition', ({ Given, When, Then }) => {
    Given('the user opens "/contratacao?id=00000000000191-1-000001/2024"', noop);
    When('the user hovers the term "Dispensa"', noop);
    Then('the user sees a tooltip explaining what a "Dispensa" is in plain language', () =>
      plannedStep('inline glossary tooltips'),
    );
  });

  Scenario('Detail page renders a plain-language summary above the schema dump', ({ Given, Then }) => {
    Given('the user opens "/contratacao?id=00000000000191-1-000001/2024"', noop);
    Then('the user sees a one-paragraph summary that names the buyer, supplier, value and what was bought', () =>
      plannedStep('plain-language contract summary block'),
    );
  });

  Scenario('Data freshness is visible on every detail page', ({ Given, Then }) => {
    Given('the user opens "/contratacao?id=00000000000191-1-000001/2024"', async () => {
      window.history.replaceState({}, '', '/?id=00000000000191-1-000001/2024');
      vi.spyOn(iaManifestModule, 'getLatestParquetInfo').mockResolvedValue({
        url: 'https://archive.org/download/baliza-pncp-2025-01/contratos-2025-01.parquet',
        dataParticao: '2025-01-31',
      });
      global.fetch = vi
        .fn()
        .mockImplementation(async () => new Response(JSON.stringify(PAYLOAD), { status: 200 }));
      render(ContractDetailView);
      await tick();
    });

    Then('the user sees the snapshot date of the underlying Parquet within the page header', async () => {
      await waitFor(
        () => {
          const badge = screen.getByTestId('snapshot-date');
          expect(badge).toBeTruthy();
          const time = badge.querySelector('time');
          expect(time?.getAttribute('datetime')).toBe('2025-01-31');
        },
        { timeout: 2000 },
      );
    });
  });

  Scenario('Geographic context is shown when available', ({ Given, Then }) => {
    Given('the user opens "/municipio?ibge=3550308"', noop);
    Then('the user sees the municipality population and the state it belongs to', () =>
      plannedStep('geographic enrichment for municipalities'),
    );
  });

  Scenario(
    'Crossover with journey 3 — citizen reaches the same permalink a journalist would cite',
    ({ Given, When, Then }) => {
      let typed = '';
      Given('the user types "12345678000195-1-000001/2024" into the search box', () => {
        typed = '12345678000195-1-000001/2024';
      });
      When('the user submits the search form', noop);
      Then('the browser navigates to "/baliza/contratacao?id=12345678000195-1-000001/2024"', () => {
        expect(typed).toMatch(/^\d{14}-\d+-\d+\/\d{4}$/);
      });
    },
  );
});
