import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, waitFor } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { tick } from 'svelte';
import { render } from './shared';
import * as parquetFallback from '../../lib/parquetFallback';
import * as cityContext from '../../lib/cityContext.svelte';
import * as geo from '../../lib/geo';
import CityWowStripRaw from '../CityWowStrip.svelte';
import CityHeroRaw from '../CityHero.svelte';

const CityWowStrip = CityWowStripRaw as unknown as Parameters<typeof render>[0];
const CityHero = CityHeroRaw as unknown as Parameters<typeof render>[0];
const feature = await loadFeature('features/city-wow-strip.feature');

const PORTO_VELHO = { ibge: '1100205', nome: 'Porto Velho', uf: 'RO' } as const;
const SAO_PAULO = { ibge: '3550308', nome: 'São Paulo', uf: 'SP' } as const;
const ALTA_FLORESTA = { ibge: '1100015', nome: "Alta Floresta D'Oeste", uf: 'RO' } as const;

function setCityState(city: { ibge: string; nome: string; uf: string }, source: 'default' | 'storage' | 'url' = 'storage') {
  cityContext.cityState.ibge = city.ibge;
  cityContext.cityState.nome = city.nome;
  cityContext.cityState.uf = city.uf;
  cityContext.cityState.source = source;
}

function mockPermission(state: PermissionState | null) {
  if (state === null) {
    Object.defineProperty(global.navigator, 'permissions', {
      configurable: true,
      writable: true,
      value: undefined,
    });
    return;
  }
  Object.defineProperty(global.navigator, 'permissions', {
    configurable: true,
    writable: true,
    value: {
      query: vi.fn().mockResolvedValue({ state, addEventListener: () => {} } as unknown as PermissionStatus),
    },
  });
}

describeFeature(feature, ({ Scenario, BeforeEachScenario }) => {
  BeforeEachScenario(async () => {
    cleanup();
    vi.restoreAllMocks();
    // Reset to default state so previous scenarios don't leak.
    setCityState(PORTO_VELHO, 'storage');
    mockPermission(null);
  });

  Scenario('Show skeleton tiles while archive aggregates are loading', ({ Given, When, Then }) => {
    Given('the city aggregates query is still pending', () => {
      vi.spyOn(parquetFallback, 'queryCityAggregates').mockReturnValue(new Promise(() => {}));
    });

    When('the wow strip mounts for "Porto Velho"', async () => {
      setCityState(PORTO_VELHO);
      render(CityWowStrip);
      await tick();
    });

    Then('I should see 4 skeleton tiles', async () => {
      await waitFor(() => {
        const tiles = document.querySelectorAll('.wow-tile--loading');
        expect(tiles.length).toBe(4);
      });
    });
  });

  Scenario('Render 4 tiles with formatted pt-BR values', ({ Given, When, Then, And }) => {
    Given('the archive returns 12847 contracts totaling R$ 4200000000 for "Porto Velho"', () => {
      vi.spyOn(parquetFallback, 'queryCityAggregates').mockResolvedValue({
        ok: true,
        rows: [
          {
            contratos: 12847,
            valorTotal: 4_200_000_000,
            ultimaPublicacao: '2026-04-15T00:00:00',
            primeiraParticao: '2023-10-01',
            ultimaParticao: '2026-04-16',
            topOrgao: { cnpj: '04722930000113', razaoSocial: 'Prefeitura Municipal', n: 3182 },
          },
        ],
        dataParticao: '2026-04-16',
      });
    });

    When('the wow strip mounts for "Porto Velho"', async () => {
      setCityState(PORTO_VELHO);
      render(CityWowStrip);
      await tick();
    });

    Then('I should see "12.847" in the tiles', async () => {
      await waitFor(() => expect(screen.getByText('12.847')).toBeTruthy(), { timeout: 2000 });
    });

    And('I should see "R$ 4.200.000.000,00" in the tiles', () => {
      // toLocaleString uses NBSP between R$ and the number; regex tolerates either.
      const match = screen.getAllByText(/R\$\s*4\.200\.000\.000,00/);
      expect(match.length).toBeGreaterThan(0);
    });

    And('the contracts tile should link to "/busca?ibge=1100205"', () => {
      const links = Array.from(document.querySelectorAll('a.wow-tile')) as HTMLAnchorElement[];
      const contractsTile = links.find((a) => a.textContent?.includes('12.847'));
      expect(contractsTile).toBeDefined();
      expect(contractsTile?.getAttribute('href')).toMatch(/busca\?ibge=1100205$/);
    });
  });

  Scenario('Empty city renders a sober zero state', ({ Given, When, Then, And }) => {
    Given('the archive returns 0 contracts for "Alta Floresta D\'Oeste"', () => {
      vi.spyOn(parquetFallback, 'queryCityAggregates').mockResolvedValue({
        ok: true,
        rows: [
          {
            contratos: 0,
            valorTotal: 0,
            ultimaPublicacao: null,
            primeiraParticao: null,
            ultimaParticao: null,
            topOrgao: null,
          },
        ],
        dataParticao: null,
      });
    });

    When('the wow strip mounts for "Alta Floresta D\'Oeste"', async () => {
      setCityState(ALTA_FLORESTA);
      render(CityWowStrip);
      await tick();
    });

    Then('I should see "0" in the tiles', async () => {
      await waitFor(() => expect(screen.getByText('0')).toBeTruthy(), { timeout: 2000 });
    });

    And('I should see an empty-state message', () => {
      expect(screen.getByText(/Nenhum contrato arquivado/i)).toBeTruthy();
    });
  });

  Scenario('Snapshot tile shows partition date and IA attribution', ({ Given, When, Then, And }) => {
    Given('the archive returns 1 contract with snapshot "2026-04-16" for "Porto Velho"', () => {
      vi.spyOn(parquetFallback, 'queryCityAggregates').mockResolvedValue({
        ok: true,
        rows: [
          {
            contratos: 1,
            valorTotal: 100,
            ultimaPublicacao: '2026-04-16',
            primeiraParticao: '2026-04-01',
            ultimaParticao: '2026-04-16',
            topOrgao: { cnpj: '04722930000113', razaoSocial: 'Prefeitura', n: 1 },
          },
        ],
        dataParticao: '2026-04-16',
      });
    });

    When('the wow strip mounts for "Porto Velho"', async () => {
      setCityState(PORTO_VELHO);
      render(CityWowStrip);
      await tick();
    });

    Then('I should see "16/04" in the snapshot tile', async () => {
      await waitFor(() => expect(screen.getByText(/16\/04/)).toBeTruthy(), { timeout: 2000 });
    });

    And('the snapshot tile should mention "Internet Archive"', () => {
      expect(screen.getByText(/Internet Archive/i)).toBeTruthy();
    });
  });

  Scenario('Hide wow strip silently when archive fails', ({ Given, When, Then }) => {
    Given('the archive query fails with "no_manifest"', () => {
      vi.spyOn(parquetFallback, 'queryCityAggregates').mockResolvedValue({
        ok: false,
        reason: 'no_manifest',
      });
    });

    When('the wow strip mounts for "Porto Velho"', async () => {
      setCityState(PORTO_VELHO);
      render(CityWowStrip);
      await tick();
    });

    Then('the wow strip section should not be in the document', async () => {
      // Wait long enough for the query to settle and the error path to render.
      await waitFor(
        () => {
          expect(document.querySelector('.wow-strip')).toBeNull();
        },
        { timeout: 2000 },
      );
    });
  });

  Scenario('Auto-detect city when geolocation permission is already granted', ({ Given, And, When, Then }) => {
    let setCitySpy: ReturnType<typeof vi.spyOn>;

    Given('navigator.permissions reports geolocation as "granted"', () => {
      mockPermission('granted');
    });

    And('the geo helper resolves to São Paulo', () => {
      vi.spyOn(geo, 'resolveCityFromBrowserLocation').mockResolvedValue(SAO_PAULO);
      setCitySpy = vi.spyOn(cityContext, 'setCity');
    });

    When('the city hero mounts with the default city source', async () => {
      setCityState(PORTO_VELHO, 'default');
      render(CityHero);
      await tick();
    });

    Then('setCity should be called with São Paulo', async () => {
      await waitFor(
        () => {
          expect(setCitySpy).toHaveBeenCalledWith(SAO_PAULO, 'storage');
        },
        { timeout: 2000 },
      );
    });
  });

  Scenario('Skip auto-detect when permission state is prompt', ({ Given, When, Then }) => {
    let setCitySpy: ReturnType<typeof vi.spyOn>;

    Given('navigator.permissions reports geolocation as "prompt"', () => {
      mockPermission('prompt');
      vi.spyOn(geo, 'resolveCityFromBrowserLocation').mockResolvedValue(SAO_PAULO);
      setCitySpy = vi.spyOn(cityContext, 'setCity');
    });

    When('the city hero mounts with the default city source', async () => {
      setCityState(PORTO_VELHO, 'default');
      render(CityHero);
      await tick();
      // Give the effect a beat to (not) run the geo flow.
      await new Promise((resolve) => setTimeout(resolve, 50));
    });

    Then('setCity should not be called', () => {
      expect(setCitySpy).not.toHaveBeenCalled();
    });
  });

  Scenario('Skip auto-detect when the visitor already has a stored city', ({ Given, And, When, Then }) => {
    let setCitySpy: ReturnType<typeof vi.spyOn>;

    Given('the active city was loaded from storage', () => {
      // setCityState below uses 'storage' source by default in this scenario.
    });

    And('navigator.permissions reports geolocation as "granted"', () => {
      mockPermission('granted');
      vi.spyOn(geo, 'resolveCityFromBrowserLocation').mockResolvedValue(SAO_PAULO);
      setCitySpy = vi.spyOn(cityContext, 'setCity');
    });

    When('the city hero mounts', async () => {
      setCityState(PORTO_VELHO, 'storage');
      render(CityHero);
      await tick();
      await new Promise((resolve) => setTimeout(resolve, 50));
    });

    Then('setCity should not be called', () => {
      expect(setCitySpy).not.toHaveBeenCalled();
    });
  });
});
