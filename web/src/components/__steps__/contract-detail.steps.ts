import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, waitFor } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { tick } from 'svelte';
import { render } from './shared';
import ContractDetailViewRaw from '../ContractDetailView.svelte';

const ContractDetailView = ContractDetailViewRaw as unknown as Parameters<typeof render>[0];
const feature = await loadFeature('features/contract-detail.feature');

function setUrlQuery(qs: string) {
  window.history.replaceState({}, '', qs ? `/?${qs}` : '/');
}

describeFeature(feature, ({ Scenario, BeforeEachScenario }) => {
  BeforeEachScenario(async () => {
    cleanup();
    vi.restoreAllMocks();
    setUrlQuery('');
  });

  Scenario('Missing ID shows EntityNotFound', ({ Given, When, Then }) => {
    Given('the URL has no id parameter', () => {
      setUrlQuery('');
    });

    When('the contract detail view mounts', async () => {
      render(ContractDetailView);
      await tick();
    });

    Then('I should see "CONTRATAÇÃO não encontrada"', async () => {
      await waitFor(() =>
        expect(screen.getByText('CONTRATAÇÃO não encontrada')).toBeTruthy(),
      );
    });
  });

  Scenario('Fetch pending shows skeleton', ({ Given, And, When, Then }) => {
    Given('the URL has id "00000000000191-1-000001-1"', () => {
      setUrlQuery('id=00000000000191-1-000001-1');
    });

    And('the fetch is pending', () => {
      global.fetch = vi.fn().mockReturnValue(new Promise(() => {}));
    });

    When('the contract detail view mounts', async () => {
      render(ContractDetailView);
      await tick();
    });

    Then('I should see an aria-busy element', async () => {
      await waitFor(() =>
        expect(document.querySelector('[aria-busy="true"]')).toBeTruthy(),
      );
    });
  });

  Scenario('Fetch error shows AlertBanner', ({ Given, And, When, Then }) => {
    Given('the URL has id "00000000000191-1-000001-1"', () => {
      setUrlQuery('id=00000000000191-1-000001-1');
    });

    And('the fetch throws "Contratação não localizada"', () => {
      global.fetch = vi
        .fn()
        .mockResolvedValue(new Response('nope', { status: 404 }));
    });

    When('the contract detail view mounts', async () => {
      render(ContractDetailView);
      await tick();
    });

    Then('I should see an alert banner with the error message', async () => {
      await waitFor(
        () => {
          const alert = screen.getByRole('alert');
          expect(alert.textContent).toMatch(/Contratação não localizada/);
        },
        { timeout: 2000 },
      );
    });
  });

  Scenario('Successful fetch renders both summary cards', ({ Given, And, When, Then }) => {
    Given('the URL has id "00000000000191-1-000001-1"', () => {
      setUrlQuery('id=00000000000191-1-000001-1');
    });

    And('the PNCP API returns a valid contract payload', () => {
      global.fetch = vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            numeroControlePNCP: '00000000000191-1-000001-1',
            dataPublicacaoPncp: '2025-01-15T00:00:00',
            objetoContratacao: 'Aquisição de materiais',
            valorTotalEstimado: 1000,
            modalidadeNome: 'Pregão',
            situacaoNome: 'Publicada',
            orgaoEntidade: { razaoSocial: 'Órgão', cnpj: '00000000000191' },
            unidadeOrgao: { nomeUnidade: 'Unidade' },
            itens: [{ sequencialItem: 1, descricao: 'Item 1' }],
          }),
          { status: 200 },
        ),
      );
    });

    When('the contract detail view mounts', async () => {
      render(ContractDetailView);
      await tick();
    });

    Then('I should see "Resumo Executivo"', async () => {
      await waitFor(
        () => expect(screen.getByText('Resumo Executivo')).toBeTruthy(),
        { timeout: 2000 },
      );
    });

    And('I should see "Itens da Licitação"', () => {
      expect(screen.getByText('Itens da Licitação')).toBeTruthy();
    });
  });
});
