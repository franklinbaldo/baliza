import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, fireEvent, waitFor } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { tick } from 'svelte';
import { render } from './shared';
import LocalBidsRaw from '../LocalBids.svelte';

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

describeFeature(feature, ({ Scenario, BeforeEachScenario }) => {
  BeforeEachScenario(async () => {
    cleanup();
    vi.restoreAllMocks();
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
});
