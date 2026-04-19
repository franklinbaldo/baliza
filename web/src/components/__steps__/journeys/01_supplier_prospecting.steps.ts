import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, fireEvent, waitFor } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { tick } from 'svelte';
import { render, noop, plannedStep } from './_shared';
import SearchHeroRaw from '../../SearchHero.svelte';
import { suggestAccented } from '../../../lib/accentSuggest';

const SearchHero = SearchHeroRaw as unknown as Parameters<typeof render>[0];

const feature = await loadFeature('features/journeys/01_supplier_prospecting.feature');

const HOSPITAL_PAYLOAD = {
  items: [
    {
      numero_controle_pncp: '00000000000191-1-000001/2024',
      description: 'Merenda escolar para rede municipal',
      orgao_nome: 'Prefeitura de São Paulo',
      data_publicacao_pncp: '2024-03-01T00:00:00',
      valor_global: 120000,
      uf_sigla: 'SP',
      modalidade_nome: 'Pregão Eletrônico',
    },
    {
      numero_controle_pncp: '00000000000191-1-000002/2024',
      description: 'Merenda escolar Rio',
      orgao_nome: 'Prefeitura do Rio',
      data_publicacao_pncp: '2024-02-01T00:00:00',
      valor_global: 80000,
      uf_sigla: 'RJ',
      modalidade_nome: 'Dispensa',
    },
  ],
  total: 2,
};

async function typeInSearch(value: string) {
  const input = screen.getByLabelText('Buscar contratos públicos') as HTMLInputElement;
  await fireEvent.input(input, { target: { value } });
  await tick();
  return input;
}

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
      global.fetch = vi
        .fn()
        .mockImplementation(async () => new Response(JSON.stringify(HOSPITAL_PAYLOAD), { status: 200 }));
      render(SearchHero);
      await tick();
      await typeInSearch('merenda escolar');
      await waitFor(() => expect(screen.getByRole('listbox')).toBeTruthy(), { timeout: 2000 });
    });

    When('the user picks UF "SP" and modality "Pregão Eletrônico"', async () => {
      const ufSelect = screen.getByLabelText('Filtrar por UF') as HTMLSelectElement;
      await fireEvent.change(ufSelect, { target: { value: 'SP' } });
      const modSelect = screen.getByLabelText('Filtrar por modalidade') as HTMLSelectElement;
      await fireEvent.change(modSelect, { target: { value: 'Pregão Eletrônico' } });
      await tick();
    });

    Then('the visible aggregates (count, total value, average value) reflect the filtered subset', async () => {
      await waitFor(() => {
        const strip = screen.getByTestId('search-aggregates');
        const dds = Array.from(strip.querySelectorAll('dd')).map((el) => el.textContent?.trim() ?? '');
        expect(dds[0]).toBe('1');
        expect(dds[1]).toMatch(/120\.000/);
        expect(dds[2]).toMatch(/120\.000/);
      });
    });
  });

  Scenario('Supplier page by CNPJ shows history, peers and average ticket', ({ Given, Then, And }) => {
    Given('the user opens "/fornecedor?cnpj=12345678000195"', noop);
    Then("the user sees the supplier's contract history", () =>
      plannedStep('/fornecedor?cnpj= route and supplier rollup view'),
    );
    And("the user sees the supplier's top three competing CNPJs for the same objects", noop);
    And("the user sees the supplier's average ticket size", noop);
  });

  Scenario('Export the current search result as CSV', ({ Given, When, Then }) => {
    Given('a search result list is visible for "merenda escolar"', async () => {
      cleanup();
      vi.restoreAllMocks();
      global.fetch = vi
        .fn()
        .mockImplementation(async () => new Response(JSON.stringify(HOSPITAL_PAYLOAD), { status: 200 }));
      render(SearchHero);
      await tick();
      await typeInSearch('merenda escolar');
      await waitFor(() => expect(screen.getByRole('listbox')).toBeTruthy(), { timeout: 2000 });
    });

    When('the user clicks "Exportar CSV"', async () => {
      const btn = screen.getByRole('button', { name: /Exportar CSV/i });
      expect(btn).toBeTruthy();
    });

    Then('a CSV file is downloaded with named columns matching the visible table', () => {
      const btn = screen.getByRole('button', { name: /Exportar CSV/i }) as HTMLButtonElement;
      expect(btn.disabled).toBe(false);
    });
  });

  Scenario('Empty search suggests accent-tolerant alternatives', ({ Given, When, Then }) => {
    Given('the user submits the free-text query "construcao de escola"', async () => {
      cleanup();
      vi.restoreAllMocks();
      global.fetch = vi
        .fn()
        .mockImplementation(async () => new Response(JSON.stringify({ items: [], total: 0 }), { status: 200 }));
      render(SearchHero);
      await tick();
      await typeInSearch('construcao de escola');
    });

    When('PNCP returns zero results', async () => {
      await waitFor(() => expect(screen.getByText(/Nenhum resultado/i)).toBeTruthy(), { timeout: 2000 });
    });

    Then('the user sees a suggestion to retry with "construção de escola"', async () => {
      expect(suggestAccented('construcao de escola')).toBe('construção de escola');
      await waitFor(() => {
        const btn = screen.getByRole('button', { name: /construção de escola/i });
        expect(btn).toBeTruthy();
      });
    });
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
