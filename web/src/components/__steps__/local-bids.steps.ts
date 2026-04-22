import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, fireEvent, waitFor } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { tick } from 'svelte';
import { render } from './shared';
import { queryParquetFallback } from '../../lib/parquetFallback';
import LocalBidsRaw from '../LocalBids.svelte';

const fallbackMock = vi.mocked(queryParquetFallback);

const LocalBids = LocalBidsRaw as unknown as Parameters<typeof render>[0];
const feature = await loadFeature('features/local-bids.feature');

function mockGeolocationError(code: number) {
  const err = Object.assign(new Error('Geolocation error'), {
    code,
    PERMISSION_DENIED: 1,
    POSITION_UNAVAILABLE: 2,
    TIMEOUT: 3,
  }) as unknown as GeolocationPositionError;
  Object.defineProperty(global.navigator, 'geolocation', {
    writable: true,
    configurable: true,
    value: {
      getCurrentPosition: vi.fn().mockImplementation(
        (_success: unknown, failure: (e: GeolocationPositionError) => void) => {
          failure(err);
        },
      ),
    },
  });
}

function mockGeolocationSuccess(lat = -10.18, lng = -48.33) {
  Object.defineProperty(global.navigator, 'geolocation', {
    writable: true,
    configurable: true,
    value: {
      getCurrentPosition: vi.fn().mockImplementation(
        (success: (pos: GeolocationPosition) => void) => {
          success({ coords: { latitude: lat, longitude: lng } } as GeolocationPosition);
        },
      ),
    },
  });
}

function mockGeoResolution() {
  mockGeolocationSuccess();
  return (url: string): Response | null => {
    if (url.includes('nominatim')) {
      return new Response(
        JSON.stringify({ address: { city: 'Palmas', state: 'Tocantins' } }),
        { status: 200 },
      );
    }
    if (url.includes('servicodados.ibge.gov.br')) {
      return new Response(
        JSON.stringify([
          {
            id: 1721000,
            nome: 'Palmas',
            microrregiao: { mesorregiao: { UF: { nome: 'Tocantins' } } },
          },
        ]),
        { status: 200 },
      );
    }
    return null;
  };
}

describeFeature(feature, ({ Scenario, BeforeEachScenario }) => {
  BeforeEachScenario(async () => {
    cleanup();
    vi.restoreAllMocks();
    fallbackMock.mockReset();
    fallbackMock.mockResolvedValue({ ok: false, reason: 'empty' });
  });

  Scenario('Initial idle state shows activate button', ({ When, Then }) => {
    When('the local bids panel loads', async () => {
      render(LocalBids);
      await tick();
    });

    Then('I should see "Ativar Localizador"', () => {
      expect(screen.getByText('Ativar Localizador')).toBeTruthy();
    });
  });

  Scenario('Permission denied shows helpful empty state', ({ Given, When, Then, And }) => {
    Given('the browser geolocation returns a PERMISSION_DENIED error', () => {
      mockGeolocationError(1);
    });

    When('the user clicks "Ativar Localizador"', async () => {
      render(LocalBids);
      await tick();
      await fireEvent.click(screen.getByText('Ativar Localizador'));
    });

    Then('I should see "Acesso à localização negado"', async () => {
      await waitFor(
        () => expect(screen.getByText('Acesso à localização negado')).toBeTruthy(),
        { timeout: 2000 },
      );
    });

    And('I should see a link to search manually', () => {
      expect(screen.getByRole('link', { name: /Buscar manualmente/i })).toBeTruthy();
    });
  });

  Scenario('General geolocation error shows error message', ({ Given, When, Then }) => {
    Given('the browser geolocation fails with a general error', () => {
      mockGeolocationError(2);
    });

    When('the user clicks "Ativar Localizador"', async () => {
      render(LocalBids);
      await tick();
      await fireEvent.click(screen.getByText('Ativar Localizador'));
    });

    Then('I should see a location failure message', async () => {
      await waitFor(
        () => expect(screen.getByText('Falha na localização')).toBeTruthy(),
        { timeout: 2000 },
      );
    });
  });

  Scenario('Success with no results shows empty state', ({ Given, And, When, Then }) => {
    Given('geolocation succeeds with IBGE "1721000"', () => {
      mockGeolocationSuccess();

      global.fetch = vi.fn().mockImplementation((url: string) => {
        if (typeof url === 'string' && url.includes('nominatim')) {
          return Promise.resolve(
            new Response(
              JSON.stringify({
                address: { city: 'Palmas', state: 'Tocantins' },
              }),
              { status: 200 },
            ),
          );
        }
        if (typeof url === 'string' && url.includes('ibge')) {
          return Promise.resolve(
            new Response(
              JSON.stringify([
                {
                  id: 1721000,
                  nome: 'Palmas',
                  microrregiao: { mesorregiao: { UF: { nome: 'Tocantins' } } },
                },
              ]),
              { status: 200 },
            ),
          );
        }
        return Promise.resolve(
          new Response(JSON.stringify({ data: [] }), { status: 200 }),
        );
      });
    });

    And('the PNCP API returns 0 results', () => {});

    When('the user activates the locator', async () => {
      render(LocalBids);
      await tick();
      await fireEvent.click(screen.getByText('Ativar Localizador'));
    });

    Then('I should see "Nenhuma contratação recente"', async () => {
      await waitFor(
        () => expect(screen.getByText('Nenhuma contratação recente')).toBeTruthy(),
        { timeout: 3000 },
      );
    });
  });

  Scenario('Success with results renders bid cards', ({ Given, And, When, Then }) => {
    Given('geolocation succeeds with IBGE "1721000"', () => {
      const resolveGeo = mockGeoResolution();

      global.fetch = vi.fn().mockImplementation((input: unknown) => {
        const url = typeof input === 'string' ? input : (input as Request).url;
        const geoResponse = resolveGeo(url);
        if (geoResponse) return Promise.resolve(geoResponse);
        return Promise.resolve(
          new Response(
            JSON.stringify({
              data: [
                {
                  numeroControlePNCP: '99999999999999-1-000001/2024',
                  dataPublicacaoPncp: '2025-01-15T00:00:00',
                  objetoContratacao: 'Contratação ao vivo',
                  valorTotalEstimado: 1000,
                  orgaoEntidade: {
                    razaoSocial: 'Órgão Local',
                    cnpj: '99999999999999',
                  },
                  unidadeOrgao: { nomeUnidade: 'Unidade' },
                },
              ],
            }),
            { status: 200 },
          ),
        );
      });
    });

    And('the PNCP API returns 1 live contract', () => {});

    When('the user activates the locator', async () => {
      render(LocalBids);
      await tick();
      await fireEvent.click(screen.getByText('Ativar Localizador'));
    });

    Then('I should see 1 bid card', async () => {
      await waitFor(
        () => {
          const cards = document.querySelectorAll('a.bid-card');
          expect(cards.length).toBe(1);
        },
        { timeout: 3000 },
      );
    });
  });

  Scenario('PNCP unavailable — archived fallback renders with info banner', ({ Given, And, When, Then }) => {
    Given('geolocation succeeds with IBGE "1721000"', () => {
      const resolveGeo = mockGeoResolution();

      global.fetch = vi.fn().mockImplementation((input: unknown) => {
        const url = typeof input === 'string' ? input : (input as Request).url;
        const geoResponse = resolveGeo(url);
        if (geoResponse) return Promise.resolve(geoResponse);
        return Promise.resolve(new Response('boom', { status: 503 }));
      });
    });

    And('every PNCP consulta call returns 503', () => {});

    And('the Parquet fallback returns 1 archived row for ibge "1721000"', () => {
      fallbackMock.mockResolvedValue({
        ok: true,
        rows: [
          {
            numero_controle_pncp: '99999999999999-1-000099/2024',
            objeto_contrato: 'Contrato arquivado – município',
            data_publicacao_pncp: '2024-08-10T00:00:00',
            valor_global: 4200,
            cnpj_orgao: '99999999999999',
            razao_social_orgao: 'Órgão Arquivado',
            codigo_ibge: '1721000',
            municipio_nome: 'Palmas',
            uf_sigla: 'TO',
            nome_unidade: 'Unidade',
          },
        ],
        dataParticao: '2024-12-01',
      });
    });

    When('the user activates the locator', async () => {
      render(LocalBids);
      await tick();
      await fireEvent.click(screen.getByText('Ativar Localizador'));
    });

    Then('I should see an info banner about PNCP indisponível', async () => {
      await waitFor(
        () => {
          const status = screen.getByRole('status');
          expect(status.textContent).toMatch(/PNCP indisponível/i);
        },
        { timeout: 3000 },
      );
    });

    And('I should see 1 bid card', async () => {
      const cards = document.querySelectorAll('a.bid-card');
      expect(cards.length).toBe(1);
    });
  });

  Scenario('Skeleton loader shown while PNCP query is pending', ({ Given, And, When, Then }) => {
    Given('geolocation succeeds with IBGE "1721000"', () => {
      const resolveGeo = mockGeoResolution();

      global.fetch = vi.fn().mockImplementation((input: unknown) => {
        const url = typeof input === 'string' ? input : (input as Request).url;
        const geoResponse = resolveGeo(url);
        if (geoResponse) return Promise.resolve(geoResponse);
        return new Promise(() => {});
      });
    });

    And('the PNCP fetch is pending', () => {});

    When('the user activates the locator', async () => {
      render(LocalBids);
      await tick();
      await fireEvent.click(screen.getByText('Ativar Localizador'));
    });

    Then('I should see a loading skeleton', async () => {
      await waitFor(
        () => {
          const busy = document.querySelector('[aria-busy="true"]');
          expect(busy).toBeTruthy();
          expect(busy?.querySelector('.skeleton')).toBeTruthy();
        },
        { timeout: 3000 },
      );
    });
  });
});
