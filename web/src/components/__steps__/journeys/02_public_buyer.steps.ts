import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { cleanup, waitFor } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { tick } from 'svelte';
import { render, noop, plannedStep } from './_shared';
import ContractDetailViewRaw from '../../ContractDetailView.svelte';

const ContractDetailView = ContractDetailViewRaw as unknown as Parameters<typeof render>[0];

const feature = await loadFeature('features/journeys/02_public_buyer.feature');

const PAYLOAD = {
  numeroControlePNCP: '00000000000191-1-000001/2024',
  dataPublicacaoPncp: '2025-01-15T00:00:00',
  objetoContratacao: 'Aquisição de merenda escolar',
  valorTotalEstimado: 1500,
  modalidadeNome: 'Pregão Eletrônico',
  orgaoEntidade: {
    razaoSocial: 'Prefeitura Municipal',
    cnpj: '00000000000191',
  },
  unidadeOrgao: {
    nomeUnidade: 'Secretaria de Compras',
    codigoMunicipioIbge: '3550308',
  },
  linkSistemaOrigem: 'https://origem.exemplo.gov.br/compras/1',
  itens: [],
};

describeFeature(feature, ({ Scenario, BeforeEachScenario }) => {
  BeforeEachScenario(async () => {
    cleanup();
    vi.restoreAllMocks();
  });

  Scenario('Browse vigent registered-price frameworks for an object', ({ Given, Then }) => {
    Given('the user opens "/atas?objeto=papel%20A4"', noop);
    Then(
      'the user sees a list of vigent atas with start date, end date, contracting agency and remaining quantity',
      () => plannedStep('atas browser route and vigent-frameworks data model'),
    );
  });

  Scenario(
    'Generate a price reference and export it as a citable PDF',
    ({ Given, When, Then, And }) => {
      Given('the user opens a market page for "papel A4"', noop);
      When('the user clicks "Gerar pesquisa de preços"', noop);
      Then('a PDF is produced containing min, average, median, max and standard deviation of unit price', () =>
        plannedStep('price-reference PDF generator'),
      );
      And('the PDF includes the source contract IDs and snapshot date', noop);
    },
  );

  Scenario(
    'Compare procurement practice across municipalities of similar size',
    ({ Given, Then, And }) => {
      Given('the user opens "/comparar?ibge=3550308&objeto=merenda"', noop);
      Then('the user sees three peer municipalities of similar population', () =>
        plannedStep('peer-municipality comparison route'),
      );
      And('the user sees the per-capita spend for the same object', noop);
    },
  );

  Scenario('Resolve a CATMAT or CATSER code from a free-text description', ({ Given, Then }) => {
    Given('the user types "papel sulfite branco A4 75g" into a catalog input', noop);
    Then('the user sees the most likely CATMAT codes ranked by match confidence', () =>
      plannedStep('CATMAT/CATSER catalog resolver'),
    );
  });

  Scenario('Inspect the legal basis cited by peers in similar exemptions', ({ Given, Then }) => {
    Given('the user opens "/dispensas?objeto=papel%20A4"', noop);
    Then('the user sees the most cited legal articles in similar dispensa contracts', () =>
      plannedStep('legal-basis aggregation across dispensa contracts'),
    );
  });

  Scenario(
    "Crossover with journey 3 — buyer audits a peer's contract before riding on it",
    ({ Given, Then, And }) => {
      Given('the user opens "/contratacao?id=00000000000191-1-000001/2024"', async () => {
        window.history.replaceState({}, '', '/?id=00000000000191-1-000001/2024');
        global.fetch = vi
          .fn()
          .mockImplementation(async () => new Response(JSON.stringify(PAYLOAD), { status: 200 }));
        render(ContractDetailView);
        await tick();
      });
      Then("the user sees the contract's value, modality and supplier", async () => {
        // ContractDetailView renders value (formatted BRL), modality and
        // orgaoEntidade.razaoSocial — the three pieces a buyer needs to
        // defend the decision against an audit.
        await waitFor(
          () => {
            const dds = Array.from(document.querySelectorAll('dd')).map(
              (el) => el.textContent?.replace(/\s+/g, ' ').trim() ?? '',
            );
            expect(dds.some((t) => /R\$\s*1\.500,00/.test(t))).toBe(true);
            expect(dds.some((t) => /Pregão Eletrônico/.test(t))).toBe(true);
            expect(dds.some((t) => /Prefeitura Municipal/.test(t))).toBe(true);
          },
          { timeout: 2000 },
        );
      });
      And('the user sees an outbound link to the original PNCP record', () => {
        const link = document.querySelector(
          'a[href="https://origem.exemplo.gov.br/compras/1"]',
        ) as HTMLAnchorElement | null;
        expect(link).toBeTruthy();
        expect(link?.getAttribute('target')).toBe('_blank');
      });
    },
  );
});
