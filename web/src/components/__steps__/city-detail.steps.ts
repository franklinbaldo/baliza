import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, waitFor } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { tick } from 'svelte';
import { render } from './shared';
import CityDetailViewRaw from '../CityDetailView.svelte';

const CityDetailView = CityDetailViewRaw as unknown as Parameters<typeof render>[0];
const feature = await loadFeature('features/city-detail.feature');

function setUrlQuery(qs: string) {
  window.history.replaceState({}, '', qs ? `/?${qs}` : '/');
}

describeFeature(feature, ({ Scenario, BeforeEachScenario }) => {
  BeforeEachScenario(async () => {
    cleanup();
    vi.restoreAllMocks();
    setUrlQuery('');
  });

  Scenario('Missing IBGE shows EntityNotFound', ({ Given, When, Then }) => {
    Given('the URL has no ibge parameter', () => {
      setUrlQuery('');
    });

    When('the city detail view mounts', async () => {
      render(CityDetailView);
      await tick();
    });

    Then('I should see "MUNICÍPIO não encontrada"', async () => {
      await waitFor(() =>
        expect(screen.getByText('MUNICÍPIO não encontrada')).toBeTruthy(),
      );
    });
  });

  Scenario('Fetch pending shows skeleton', ({ Given, And, When, Then }) => {
    Given('the URL has ibge "1721000"', () => {
      setUrlQuery('ibge=1721000');
    });

    And('the fetch is pending', () => {
      global.fetch = vi.fn().mockReturnValue(new Promise(() => {}));
    });

    When('the city detail view mounts', async () => {
      render(CityDetailView);
      await tick();
    });

    Then('I should see an aria-busy element', async () => {
      await waitFor(() =>
        expect(document.querySelector('[aria-busy="true"]')).toBeTruthy(),
      );
    });
  });

  Scenario('Fetch error shows AlertBanner', ({ Given, And, When, Then }) => {
    Given('the URL has ibge "1721000"', () => {
      setUrlQuery('ibge=1721000');
    });

    And('the fetch throws "Município não localizado"', () => {
      global.fetch = vi
        .fn()
        .mockResolvedValue(new Response('nope', { status: 404 }));
    });

    When('the city detail view mounts', async () => {
      render(CityDetailView);
      await tick();
    });

    Then('I should see an alert banner with the error message', async () => {
      await waitFor(
        () => {
          const alert = screen.getByRole('alert');
          expect(alert.textContent).toMatch(/Município não localizado/);
        },
        { timeout: 2000 },
      );
    });
  });

  Scenario('Zero contracts shows EmptyState', ({ Given, And, When, Then }) => {
    Given('the URL has ibge "1721000"', () => {
      setUrlQuery('ibge=1721000');
    });

    And('the PNCP API returns 0 contracts', () => {
      global.fetch = vi
        .fn()
        .mockResolvedValue(
          new Response(JSON.stringify({ data: [] }), { status: 200 }),
        );
    });

    When('the city detail view mounts', async () => {
      render(CityDetailView);
      await tick();
    });

    Then('I should see "Nenhuma contratação recente"', async () => {
      await waitFor(
        () =>
          expect(screen.getByText('Nenhuma contratação recente')).toBeTruthy(),
        { timeout: 2000 },
      );
    });
  });

  Scenario('Contract count rendered as stat card', ({ Given, And, When, Then }) => {
    Given('the URL has ibge "1721000"', () => {
      setUrlQuery('ibge=1721000');
    });

    And('the PNCP API returns 1 contract', () => {
      global.fetch = vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            data: [
              {
                numeroControlePNCP: '99999999999999-1-000001-1',
                dataPublicacaoPncp: '2025-01-15T00:00:00',
                objetoContratacao: 'Teste',
                valorTotalEstimado: 500,
                orgaoEntidade: { razaoSocial: 'Órgão', cnpj: '99999999999999' },
                unidadeOrgao: { nomeUnidade: 'Unidade' },
                municipio: { nome: 'Palmas', uf: 'TO' },
              },
            ],
          }),
          { status: 200 },
        ),
      );
    });

    When('the city detail view mounts', async () => {
      render(CityDetailView);
      await tick();
    });

    Then('I should see a stat card with value "1"', async () => {
      await waitFor(
        () => {
          expect(screen.getByText('Contratações Recentes')).toBeTruthy();
          expect(screen.getByText('1')).toBeTruthy();
        },
        { timeout: 2000 },
      );
    });
  });
});
