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

function daysBetweenYyyymmdd(ini: string, fim: string): number {
  const parse = (s: string) =>
    Date.UTC(Number(s.slice(0, 4)), Number(s.slice(4, 6)) - 1, Number(s.slice(6, 8)));
  return Math.round((parse(fim) - parse(ini)) / 86_400_000);
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
      'the PNCP API returns a contract with id "00000000000191-1-000001/2024"',
      () => {
        global.fetch = vi.fn().mockResolvedValue(
          new Response(
            JSON.stringify({
              data: [
                {
                  numeroControlePNCP: '00000000000191-1-000001/2024',
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
            'a[href*="00000000000191-1-000001/2024"]',
          );
          expect(link).toBeTruthy();
        },
        { timeout: 2000 },
      );
    });
  });

  Scenario('Query string uses cnpj, not cnpjOrgao', ({ Given, And, When, Then }) => {
    Given('the URL has cnpj "00000000000191"', () => {
      setUrlQuery('cnpj=00000000000191');
    });

    And('the PNCP API returns 0 contracts', () => {
      global.fetch = vi
        .fn()
        .mockResolvedValue(new Response(JSON.stringify({ data: [] }), { status: 200 }));
    });

    When('the agency detail view mounts', async () => {
      render(AgencyDetailView);
      await tick();
    });

    Then('every PNCP consulta URL should contain "cnpj=00000000000191"', async () => {
      await waitFor(
        () => {
          const fetchMock = global.fetch as unknown as ReturnType<typeof vi.fn>;
          expect(fetchMock.mock.calls.length).toBeGreaterThan(0);
          for (const call of fetchMock.mock.calls) {
            const url = typeof call[0] === 'string' ? call[0] : (call[0] as Request).url;
            expect(url).toMatch(/[?&]cnpj=00000000000191\b/);
          }
        },
        { timeout: 2000 },
      );
    });

    And('no PNCP consulta URL should contain "cnpjOrgao="', () => {
      const fetchMock = global.fetch as unknown as ReturnType<typeof vi.fn>;
      for (const call of fetchMock.mock.calls) {
        const url = typeof call[0] === 'string' ? call[0] : (call[0] as Request).url;
        expect(url).not.toMatch(/cnpjOrgao=/);
      }
    });
  });

  Scenario('Lookback window dias=180 widens the publicação date range', ({ Given, And, When, Then }) => {
    Given('the URL has cnpj "00000000000191" and dias "180"', () => {
      setUrlQuery('cnpj=00000000000191&dias=180');
    });

    And('the PNCP API returns 0 contracts', () => {
      global.fetch = vi
        .fn()
        .mockResolvedValue(new Response(JSON.stringify({ data: [] }), { status: 200 }));
    });

    When('the agency detail view mounts', async () => {
      render(AgencyDetailView);
      await tick();
    });

    Then('every PNCP consulta URL should span 180 days between dataInicial and dataFinal', async () => {
      await waitFor(
        () => {
          const fetchMock = global.fetch as unknown as ReturnType<typeof vi.fn>;
          expect(fetchMock.mock.calls.length).toBeGreaterThan(0);
          for (const call of fetchMock.mock.calls) {
            const url = typeof call[0] === 'string' ? call[0] : (call[0] as Request).url;
            const ini = /dataInicial=(\d{8})/.exec(url)?.[1];
            const fim = /dataFinal=(\d{8})/.exec(url)?.[1];
            expect(ini && fim).toBeTruthy();
            expect(daysBetweenYyyymmdd(ini as string, fim as string)).toBe(180);
          }
        },
        { timeout: 2000 },
      );
    });
  });
});
