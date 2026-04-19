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
          expect(alert.textContent).toMatch(/PNCP indisponível/);
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

  Scenario('Fan-out merges contracts from multiple modalities and dedupes', ({ Given, And, When, Then }) => {
    Given('the URL has ibge "1721000"', () => {
      setUrlQuery('ibge=1721000');
    });

    And('the PNCP API returns 2 contracts for modality 6 and 1 contract for modality 8', () => {
      const makeContract = (id: string) => ({
        numeroControlePNCP: id,
        dataPublicacaoPncp: '2025-01-15T00:00:00',
        objetoContratacao: 'Teste fan-out',
        valorTotalEstimado: 100,
        orgaoEntidade: { razaoSocial: 'Órgão', cnpj: '99999999999999' },
        unidadeOrgao: { nomeUnidade: 'Unidade' },
      });
      global.fetch = vi.fn().mockImplementation(async (input: unknown) => {
        const url = typeof input === 'string' ? input : (input as Request).url;
        const match = /codigoModalidadeContratacao=(\d+)/.exec(url);
        const mod = match ? Number(match[1]) : null;
        if (mod === 6) {
          return new Response(
            JSON.stringify({ data: [makeContract('99999999999999-1-000001/2024'), makeContract('99999999999999-1-000002/2024')] }),
            { status: 200 },
          );
        }
        if (mod === 8) {
          return new Response(
            JSON.stringify({ data: [makeContract('99999999999999-1-000003/2024')] }),
            { status: 200 },
          );
        }
        return new Response(JSON.stringify({ data: [] }), { status: 200 });
      });
    });

    When('the city detail view mounts', async () => {
      render(CityDetailView);
      await tick();
    });

    Then('I should see a stat card with value "3"', async () => {
      await waitFor(
        () => {
          expect(screen.getByText('Contratações Recentes')).toBeTruthy();
          expect(screen.getByText('3')).toBeTruthy();
        },
        { timeout: 2000 },
      );
    });
  });

  Scenario('Every PNCP consulta URL sends the required publicação parameters', ({ Given, And, When, Then }) => {
    Given('the URL has ibge "1721000"', () => {
      setUrlQuery('ibge=1721000');
    });

    And('the PNCP API returns 0 contracts', () => {
      global.fetch = vi
        .fn()
        .mockResolvedValue(new Response(JSON.stringify({ data: [] }), { status: 200 }));
    });

    When('the city detail view mounts', async () => {
      render(CityDetailView);
      await tick();
    });

    Then('every PNCP consulta URL should include dataInicial, dataFinal, codigoModalidadeContratacao and pagina', async () => {
      await waitFor(
        () => {
          const fetchMock = global.fetch as unknown as ReturnType<typeof vi.fn>;
          expect(fetchMock.mock.calls.length).toBeGreaterThan(0);
          for (const call of fetchMock.mock.calls) {
            const url = typeof call[0] === 'string' ? call[0] : (call[0] as Request).url;
            expect(url).toMatch(/dataInicial=\d{8}/);
            expect(url).toMatch(/dataFinal=\d{8}/);
            expect(url).toMatch(/codigoModalidadeContratacao=\d+/);
            expect(url).toMatch(/pagina=1\b/);
          }
        },
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
                numeroControlePNCP: '99999999999999-1-000001/2024',
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
