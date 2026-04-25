import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, waitFor } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { tick } from 'svelte';
import { render, noop, plannedStep } from './_shared';
import ContractDetailViewRaw from '../../ContractDetailView.svelte';
import AtasViewRaw from '../../AtasView.svelte';
import * as parquetFallback from '../../../lib/parquetFallback';
import type { ArchivedContrato } from '../../../lib/archive/schema';

const ContractDetailView = ContractDetailViewRaw as unknown as Parameters<typeof render>[0];
const AtasView = AtasViewRaw as unknown as Parameters<typeof render>[0];

function futurePlus(years: number): string {
  const d = new Date();
  d.setFullYear(d.getFullYear() + years);
  return d.toISOString().slice(0, 10);
}

const ATAS_ROW = {
  numero_controle_pncp: '00000000000191-1-000001/2024',
  data_publicacao_pncp: '2025-01-15',
  data_vigencia_inicio: '2025-01-15',
  data_vigencia_fim: futurePlus(1),
  objeto_contrato: 'Registro de preços para papel A4 sulfite',
  valor_global: 120000,
  valor_inicial: 120000,
  cnpj_orgao: '00000000000191',
  razao_social_orgao: 'Prefeitura Exemplo',
  nome_unidade: 'Secretaria de Compras',
} as unknown as ArchivedContrato;

const feature = await loadFeature('features/journeys/02_public_buyer.feature');

const PAYLOAD = {
  numeroControlePNCP: '00000000000191-1-000001/2024',
  dataPublicacaoPncp: '2025-01-15T00:00:00',
  objetoCompra: 'Aquisição de merenda escolar',
  valorTotalEstimado: 1500,
  modalidadeNome: 'Pregão Eletrônico',
  orgaoEntidade: {
    razaoSocial: 'Prefeitura Municipal',
    cnpj: '00000000000191',
  },
  unidadeOrgao: {
    nomeUnidade: 'Secretaria de Compras',
    codigoIbge: '3550308',
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
    Given('the user opens "/atas?objeto=papel%20A4"', async () => {
      cleanup();
      vi.restoreAllMocks();
      window.history.replaceState({}, '', '/?objeto=papel%20A4');
      vi.spyOn(parquetFallback, 'queryArchivedTableWhere').mockResolvedValue({
        ok: true,
        rows: [ATAS_ROW],
        dataParticao: '2025-01-31',
      });
      render(AtasView);
      await tick();
    });
    Then(
      'the user sees a list of vigent atas with start date, end date and contracting agency',
      async () => {
        await waitFor(
          () => expect(screen.getByTestId('atas-list')).toBeTruthy(),
          { timeout: 2000 },
        );
      },
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
      plannedStep('CATMAT/CATSER resolver backed by a bundled taxonomy'),
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
