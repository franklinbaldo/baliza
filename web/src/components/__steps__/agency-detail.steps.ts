import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, waitFor } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { tick } from 'svelte';
import { render } from './shared';
import AgencyDetailViewRaw from '../AgencyDetailView.svelte';

const AgencyDetailView = AgencyDetailViewRaw as unknown as Parameters<typeof render>[0];
const feature = await loadFeature('features/agency-detail.feature');

function setUrlQuery(qs: string) {
  window.history.replaceState({}, '', qs ? `/?${qs}` : '/');
}

describeFeature(feature, ({ Scenario, BeforeEachScenario }) => {
  BeforeEachScenario(async () => {
    cleanup();
    vi.restoreAllMocks();
    setUrlQuery('');
  });

  Scenario('Missing CNPJ shows EntityNotFound', ({ Given, When, Then }) => {
    Given('the URL has no cnpj parameter', () => {
      setUrlQuery('');
    });

    When('the agency detail view mounts', async () => {
      render(AgencyDetailView);
      await tick();
    });

    Then('I should see "ÓRGÃO não encontrada"', async () => {
      await waitFor(() =>
        expect(screen.getByText('ÓRGÃO não encontrada')).toBeTruthy(),
      );
    });
  });

  Scenario('Fetch pending shows skeleton', ({ Given, And, When, Then }) => {
    Given('the URL has cnpj "00000000000191"', () => {
      setUrlQuery('cnpj=00000000000191');
    });

    And('the fetch is pending', () => {
      global.fetch = vi.fn().mockReturnValue(new Promise(() => {}));
    });

    When('the agency detail view mounts', async () => {
      render(AgencyDetailView);
      await tick();
    });

    Then('I should see an aria-busy element', async () => {
      await waitFor(() =>
        expect(document.querySelector('[aria-busy="true"]')).toBeTruthy(),
      );
    });
  });

  Scenario('Fetch error shows AlertBanner', ({ Given, And, When, Then }) => {
    Given('the URL has cnpj "00000000000191"', () => {
      setUrlQuery('cnpj=00000000000191');
    });

    And('the fetch throws "Órgão não localizado"', () => {
      global.fetch = vi
        .fn()
        .mockResolvedValue(new Response('nope', { status: 404 }));
    });

    When('the agency detail view mounts', async () => {
      render(AgencyDetailView);
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
    Given('the URL has cnpj "00000000000191"', () => {
      setUrlQuery('cnpj=00000000000191');
    });

    And('the PNCP API returns 0 contracts', () => {
      global.fetch = vi
        .fn()
        .mockResolvedValue(
          new Response(JSON.stringify({ data: [] }), { status: 200 }),
        );
    });

    When('the agency detail view mounts', async () => {
      render(AgencyDetailView);
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

  Scenario('Contracts render as links', ({ Given, And, When, Then }) => {
    Given('the URL has cnpj "00000000000191"', () => {
      setUrlQuery('cnpj=00000000000191');
    });

    And(
      'the PNCP API returns a contract with id "00000000000191-1-000001-1"',
      () => {
        global.fetch = vi.fn().mockResolvedValue(
          new Response(
            JSON.stringify({
              data: [
                {
                  numeroControlePNCP: '00000000000191-1-000001-1',
                  dataPublicacaoPncp: '2025-01-15T00:00:00',
                  objetoContratacao: 'Aquisição de materiais de teste',
                  valorTotalEstimado: 1000,
                  orgaoEntidade: {
                    razaoSocial: 'Órgão Teste',
                    cnpj: '00000000000191',
                  },
                  unidadeOrgao: { nomeUnidade: 'Unidade' },
                },
              ],
            }),
            { status: 200 },
          ),
        );
      },
    );

    When('the agency detail view mounts', async () => {
      render(AgencyDetailView);
      await tick();
    });

    Then('I should see a link to that contract', async () => {
      await waitFor(
        () => {
          const link = document.querySelector(
            'a[href*="00000000000191-1-000001-1"]',
          );
          expect(link).toBeTruthy();
        },
        { timeout: 2000 },
      );
    });
  });
});
