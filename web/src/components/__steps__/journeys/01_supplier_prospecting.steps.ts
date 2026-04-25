import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, waitFor, fireEvent } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { tick } from 'svelte';
import { render, noop, plannedStep } from './_shared';
import SupplierDetailViewRaw from '../../SupplierDetailView.svelte';
import BuscaViewRaw from '../../BuscaView.svelte';
import * as parquetFallback from '../../../lib/parquetFallback';
import * as pncpPublicacao from '../../../lib/pncpPublicacao';
import type { PNCPContract } from '../../../lib/pncp';

const SupplierDetailView = SupplierDetailViewRaw as unknown as Parameters<typeof render>[0];
const BuscaView = BuscaViewRaw as unknown as Parameters<typeof render>[0];

const SUPPLIER_ROWS = [
  {
    numero_controle_pncp: '00000000000191-1-000001/2024',
    data_publicacao_pncp: '2025-01-15',
    objeto_contrato: 'Fornecimento de materiais de escritório',
    valor_global: 1000,
    valor_inicial: 1000,
    cnpj_orgao: '00000000000191',
    razao_social_orgao: 'Prefeitura Exemplo',
    nome_unidade: 'Secretaria de Compras',
    ni_fornecedor: '12345678000195',
    nome_razao_social_fornecedor: 'Fornecedora Exemplo Ltda',
  },
  {
    numero_controle_pncp: '00000000000191-1-000002/2024',
    data_publicacao_pncp: '2025-02-10',
    objeto_contrato: 'Fornecimento de materiais de escritório',
    valor_global: 2000,
    valor_inicial: 2000,
    cnpj_orgao: '00000000000191',
    razao_social_orgao: 'Prefeitura Exemplo',
    nome_unidade: 'Secretaria de Compras',
    ni_fornecedor: '12345678000195',
    nome_razao_social_fornecedor: 'Fornecedora Exemplo Ltda',
  },
];

const feature = await loadFeature('features/journeys/01_supplier_prospecting.feature');

describeFeature(feature, ({ Scenario }) => {
  Scenario('Search for an object opens an aggregated market page', ({ Given, When, Then, And }) => {
    Given('the user opens the home page', noop);
    When('the user submits the free-text query "merenda escolar"', noop);
    Then('the user lands on a market page summarizing top buyers, top suppliers and price ranges', () =>
      plannedStep('market aggregation page (/mercado/{objeto})'),
    );
    And('the page shows the number of distinct contracts found', noop);
  });

  Scenario('Filter contracts by UF and modality recomputes aggregates', ({ Given, When, Then }) => {
    Given('a search result list is visible', async () => {
      cleanup();
      vi.restoreAllMocks();
      window.history.replaceState({}, '', '/busca?q=merenda%20escolar');
      // Four results spanning two UFs (SP, RJ) and two modalities. The
      // SP+Pregão Eletrônico subset is two rows whose aggregate matches
      // the assertions below — count 2, total 3500, average 1750.
      vi.spyOn(pncpPublicacao, 'fetchPublicacaoPagesForObjeto').mockResolvedValue([
        {
          numeroControlePNCP: '00000000000191-1-000001/2024',
          dataPublicacaoPncp: '2025-01-10T00:00:00',
          objetoContratacao: 'Merenda escolar — banana e leite',
          valorTotalEstimado: 1500,
          modalidadeNome: 'Pregão Eletrônico',
          orgaoEntidade: { razaoSocial: 'Prefeitura SP-A', cnpj: '00000000000191' },
          unidadeOrgao: { nomeUnidade: 'Educação', ufSigla: 'SP' },
        },
        {
          numeroControlePNCP: '00000000000191-1-000002/2024',
          dataPublicacaoPncp: '2025-01-12T00:00:00',
          objetoContratacao: 'Merenda escolar — pão',
          valorTotalEstimado: 2000,
          modalidadeNome: 'Pregão Eletrônico',
          orgaoEntidade: { razaoSocial: 'Prefeitura SP-B', cnpj: '00000000000192' },
          unidadeOrgao: { nomeUnidade: 'Educação', ufSigla: 'SP' },
        },
        {
          numeroControlePNCP: '00000000000191-1-000003/2024',
          dataPublicacaoPncp: '2025-01-13T00:00:00',
          objetoContratacao: 'Merenda escolar — frutas',
          valorTotalEstimado: 3000,
          modalidadeNome: 'Pregão Eletrônico',
          orgaoEntidade: { razaoSocial: 'Prefeitura RJ', cnpj: '00000000000193' },
          unidadeOrgao: { nomeUnidade: 'Educação', ufSigla: 'RJ' },
        },
        {
          numeroControlePNCP: '00000000000191-1-000004/2024',
          dataPublicacaoPncp: '2025-01-14T00:00:00',
          objetoContratacao: 'Merenda escolar — laticínios',
          valorTotalEstimado: 5000,
          modalidadeNome: 'Tomada de Preços',
          orgaoEntidade: { razaoSocial: 'Prefeitura SP-C', cnpj: '00000000000194' },
          unidadeOrgao: { nomeUnidade: 'Educação', ufSigla: 'SP' },
        },
      ] as unknown as PNCPContract[]);
      render(BuscaView);
      await tick();
      // Wait until the unfiltered aggregate strip has rendered with all 4 rows.
      await waitFor(
        () => {
          const count = screen.getByTestId('busca-aggregate-count');
          expect(count.textContent).toBe('4');
        },
        { timeout: 3000 },
      );
    });

    When('the user picks UF "SP" and modality "Pregão Eletrônico"', async () => {
      const uf = screen.getByTestId('busca-filter-uf') as HTMLSelectElement;
      uf.value = 'SP';
      await fireEvent.change(uf);
      const modality = screen.getByTestId('busca-filter-modality') as HTMLSelectElement;
      modality.value = 'Pregão Eletrônico';
      await fireEvent.change(modality);
    });

    Then('the visible aggregates (count, total value, average value) reflect the filtered subset', async () => {
      await waitFor(
        () => {
          // SP + Pregão Eletrônico narrows to the 1500 + 2000 rows.
          expect(screen.getByTestId('busca-aggregate-count').textContent).toBe('2');
          expect(screen.getByTestId('busca-aggregate-total').textContent).toMatch(/3\.500,00/);
          expect(screen.getByTestId('busca-aggregate-average').textContent).toMatch(/1\.750,00/);
        },
        { timeout: 2000 },
      );
    });
  });

  Scenario('Empty search suggests accent-tolerant alternatives', ({ Given, When, Then }) => {
    Given('the user submits the free-text query "construcao de escola"', async () => {
      cleanup();
      vi.restoreAllMocks();
      window.history.replaceState({}, '', '/busca?q=construcao%20de%20escola');
      vi.spyOn(pncpPublicacao, 'fetchPublicacaoPagesForObjeto').mockResolvedValue([]);
      render(BuscaView);
      await tick();
    });
    When('PNCP returns zero results', noop);
    Then('the user sees a suggestion to retry with "construção de escola"', async () => {
      await waitFor(
        () => {
          const link = screen.getByTestId('busca-suggestion');
          expect(link.textContent).toBe('construção de escola');
        },
        { timeout: 3000 },
      );
    });
  });

  Scenario('Supplier page by CNPJ shows history, peers and average ticket', ({ Given, Then, And }) => {
    Given('the user opens "/fornecedor?cnpj=12345678000195"', async () => {
      vi.restoreAllMocks();
      window.history.replaceState({}, '', '/?cnpj=12345678000195');
      vi.spyOn(parquetFallback, 'queryParquetFallback').mockResolvedValue({
        ok: true,
        rows: SUPPLIER_ROWS,
        dataParticao: '2025-01-31',
      });
      render(SupplierDetailView);
      await tick();
    });

    Then("the user sees the supplier's contract history", async () => {
      await waitFor(
        () => expect(screen.getByTestId('contract-history')).toBeTruthy(),
        { timeout: 2000 },
      );
    });

    And("the user sees the supplier's top three competing CNPJs for the same objects", async () => {
      await waitFor(
        () => expect(screen.getByTestId('competing-suppliers')).toBeTruthy(),
        { timeout: 2000 },
      );
    });

    And("the user sees the supplier's average ticket size", async () => {
      await waitFor(
        () => {
          const card = screen.getByTestId('avg-ticket');
          expect(card.textContent).toMatch(/R\$\s*1\.500,00/);
        },
        { timeout: 2000 },
      );
    });
  });

  Scenario('Export the current search result as CSV', ({ Given, When, Then }) => {
    Given('a search result list is visible for "merenda escolar"', noop);
    When('the user clicks "Exportar CSV"', noop);
    Then('a CSV file is downloaded with named columns matching the visible table', () =>
      plannedStep('dedicated search page with CSV export'),
    );
  });

  Scenario(
    'Crossover with journey 2 — supplier inspects the buyer\'s pricing reference',
    ({ Given, When, Then }) => {
      Given('a market page for "merenda escolar" is visible', noop);
      When('the user clicks "Ver pesquisa de preços"', noop);
      Then('the user sees the same evidenced price reference shown to public buyers', () =>
        plannedStep('shared price-reference module across market and buyer pages'),
      );
    },
  );
});
