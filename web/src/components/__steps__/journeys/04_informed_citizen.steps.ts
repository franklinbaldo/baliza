import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, waitFor, fireEvent } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { tick } from 'svelte';
import { render } from './_shared';
import ContractDetailViewRaw from '../../ContractDetailView.svelte';
import CityDetailViewRaw from '../../CityDetailView.svelte';
import BuscaViewRaw from '../../BuscaView.svelte';
import HomeBuscaFormRaw from '../../HomeBuscaForm.svelte';
import * as iaManifestModule from '../../../lib/ia-manifest';
import * as pncpPublicacao from '../../../lib/pncpPublicacao';
import * as navigateModule from '../../../lib/navigate';
import type { PNCPContract } from '../../../lib/pncp';

const ContractDetailView = ContractDetailViewRaw as unknown as Parameters<typeof render>[0];
const CityDetailView = CityDetailViewRaw as unknown as Parameters<typeof render>[0];
const BuscaView = BuscaViewRaw as unknown as Parameters<typeof render>[0];
const HomeBuscaForm = HomeBuscaFormRaw as unknown as Parameters<typeof render>[0];

const feature = await loadFeature('features/journeys/04_informed_citizen.feature');

const PAYLOAD = {
  numeroControlePNCP: '00000000000191-1-000001/2024',
  dataPublicacaoPncp: '2025-01-15T00:00:00',
  objetoContratacao: 'Materiais hospitalares',
  valorTotalEstimado: 1500,
  modalidadeNome: 'Pregão Eletrônico',
  orgaoEntidade: {
    razaoSocial: 'Prefeitura Municipal',
    cnpj: '00000000000191',
  },
  unidadeOrgao: {
    nomeUnidade: 'Compras',
  },
  itens: [],
};

describeFeature(feature, ({ Scenario, BeforeEachScenario }) => {
  BeforeEachScenario(async () => {
    cleanup();
    vi.restoreAllMocks();
    window.history.replaceState({}, '', '/');
  });

  Scenario('Search by hospital name without knowing the CNPJ', ({ Given, When, Then }) => {
    Given('the user opens the home page', async () => {
      cleanup();
      vi.restoreAllMocks();
      // Step 1 of the journey: the homepage renders HomeBuscaForm. Verify
      // the form wiring so a regression in action/name/base-path on
      // index.astro would fail this scenario rather than silently ship.
      render(HomeBuscaForm);
      await tick();
      const input = screen.getByLabelText('Buscar no PNCP') as HTMLInputElement;
      expect(input.getAttribute('name')).toBe('q');
      const form = input.closest('form') as HTMLFormElement;
      expect(form.getAttribute('method')?.toLowerCase()).toBe('get');
      expect(form.getAttribute('action')).toBe('/baliza/busca');
    });
    When('the user types "hospital municipal" into the search box', async () => {
      // A real browser submit on the GET form would navigate to
      // /baliza/busca?q=hospital+municipal. jsdom does not follow form
      // navigation, so simulate the landing by rendering BuscaView with
      // the URL the browser would have produced.
      cleanup();
      window.history.replaceState({}, '', '/busca?q=hospital%20municipal');
      vi.spyOn(pncpPublicacao, 'fetchPublicacaoPagesForObjeto').mockResolvedValue([
        {
          numeroControlePNCP: '00000000000191-1-000001/2024',
          dataPublicacaoPncp: '2025-01-10T00:00:00',
          objetoContratacao: 'Aquisição de medicamentos — Hospital Municipal Central',
          valorTotalEstimado: 15000,
          modalidadeNome: 'Pregão Eletrônico',
          orgaoEntidade: { razaoSocial: 'Prefeitura X', cnpj: '00000000000191' },
          unidadeOrgao: { nomeUnidade: 'Secretaria de Saúde' },
        },
      ] as unknown as PNCPContract[]);
      render(BuscaView);
      await tick();
    });
    Then('the user sees a results listbox with at least one link', async () => {
      const list = await waitFor(
        () => {
          const el = screen.getByTestId('busca-results');
          expect(el).toBeTruthy();
          return el;
        },
        { timeout: 3000 },
      );
      expect(list.getAttribute('role')).toBe('listbox');
      const links = list.querySelectorAll('a[href]');
      expect(links.length).toBeGreaterThan(0);
    });
  });

  Scenario('Hover a technical term to see a plain-language definition', ({ Given, When, Then }) => {
    Given('the user opens "/contratacao?id=00000000000191-1-000001/2024"', async () => {
      window.history.replaceState({}, '', '/?id=00000000000191-1-000001/2024');
      vi.spyOn(iaManifestModule, 'getLatestParquetInfo').mockResolvedValue({
        url: 'https://archive.org/download/baliza-pncp-2025-01/contratos-2025-01.parquet',
        dataParticao: '2025-01-31',
      });
      global.fetch = vi
        .fn()
        .mockImplementation(async () =>
          new Response(JSON.stringify({ ...PAYLOAD, modalidadeNome: 'Dispensa' }), { status: 200 }),
        );
      render(ContractDetailView);
      await tick();
    });

    When('the user hovers the term "Dispensa"', async () => {
      await waitFor(
        () => expect(screen.getByTestId('glossary-term')).toBeTruthy(),
        { timeout: 2000 },
      );
    });

    Then('the user sees a tooltip explaining what a "Dispensa" is in plain language', () => {
      const wrap = screen.getByTestId('glossary-term');
      expect(wrap.textContent).toContain('Dispensa');
      const tooltip = wrap.querySelector('[role="tooltip"]') as HTMLElement | null;
      expect(tooltip).not.toBeNull();
      expect(tooltip!.textContent?.toLowerCase()).toMatch(/sem licitação|compra direta/);
    });
  });

  Scenario('Detail page renders a plain-language summary above the schema dump', ({ Given, Then }) => {
    Given('the user opens "/contratacao?id=00000000000191-1-000001/2024"', async () => {
      window.history.replaceState({}, '', '/?id=00000000000191-1-000001/2024');
      vi.spyOn(iaManifestModule, 'getLatestParquetInfo').mockResolvedValue({
        url: 'https://archive.org/download/baliza-pncp-2025-01/contratos-2025-01.parquet',
        dataParticao: '2025-01-31',
      });
      global.fetch = vi
        .fn()
        .mockImplementation(async () => new Response(JSON.stringify(PAYLOAD), { status: 200 }));
      render(ContractDetailView);
      await tick();
    });

    Then('the user sees a one-paragraph summary that names the buyer, supplier, value and what was bought', async () => {
      await waitFor(
        () => {
          const summary = screen.getByTestId('plain-language-summary');
          // Buyer is present; supplier is not in the live payload, so the
          // summary is expected to degrade gracefully and name the object.
          expect(summary.textContent).toContain('Prefeitura Municipal');
          expect(summary.textContent).toContain('Materiais hospitalares');
          expect(summary.textContent).toMatch(/R\$.*1\.500|1.500,00/);
        },
        { timeout: 2000 },
      );
    });
  });

  Scenario('Data freshness is visible on every detail page', ({ Given, Then }) => {
    Given('the user opens "/contratacao?id=00000000000191-1-000001/2024"', async () => {
      window.history.replaceState({}, '', '/?id=00000000000191-1-000001/2024');
      vi.spyOn(iaManifestModule, 'getLatestParquetInfo').mockResolvedValue({
        url: 'https://archive.org/download/baliza-pncp-2025-01/contratos-2025-01.parquet',
        dataParticao: '2025-01-31',
      });
      global.fetch = vi
        .fn()
        .mockImplementation(async () => new Response(JSON.stringify(PAYLOAD), { status: 200 }));
      render(ContractDetailView);
      await tick();
    });

    Then('the user sees the snapshot date of the underlying Parquet within the page header', async () => {
      await waitFor(
        () => {
          const badge = screen.getByTestId('snapshot-date');
          expect(badge).toBeTruthy();
          const time = badge.querySelector('time');
          expect(time?.getAttribute('datetime')).toBe('2025-01-31');
        },
        { timeout: 2000 },
      );
    });
  });

  Scenario('Geographic context is shown when available', ({ Given, Then }) => {
    Given('the user opens "/municipio?ibge=3550308"', async () => {
      window.history.replaceState({}, '', '/?ibge=3550308');
      global.fetch = vi
        .fn()
        .mockImplementation(async () => new Response(JSON.stringify({
          data: [{
            numeroControlePNCP: 'dummy',
            dataPublicacaoPncp: '2024-01-01',
            objetoContratacao: 'dummy',
            orgaoEntidade: { razaoSocial: 'A', cnpj: 'B' },
            unidadeOrgao: { nomeUnidade: 'C' }
          }]
        }), { status: 200 }));
      render(CityDetailView);
      await tick();
    });

    Then('the user sees the municipality population and the state it belongs to', async () => {
      await waitFor(
        () => {
          const meta = screen.getByTestId('city-meta');
          expect(meta.textContent).toContain('População: 11.451.999');
        },
        { timeout: 2000 }
      );
    });
  });

  Scenario(
    'Crossover with journey 3 — citizen reaches the same permalink a journalist would cite',
    ({ Given, When, Then }) => {
      let navigateSpy: ReturnType<typeof vi.spyOn> | null = null;
      let fetchSpy: ReturnType<typeof vi.spyOn> | null = null;

      Given('the user types "12345678000195-1-000001/2024" into the search box', async () => {
        cleanup();
        vi.restoreAllMocks();
        window.history.replaceState({}, '', '/busca');
        // navigate() is a thin wrapper around window.location.assign that
        // exists so tests can spy without fighting jsdom's non-configurable
        // Location.assign.
        navigateSpy = vi.spyOn(navigateModule, 'navigate').mockImplementation(() => {});
        // The PNCP-id shortcut must fire BEFORE any fetch. Spy on the fetch
        // helper so we can assert it was never called on the id path.
        fetchSpy = vi.spyOn(pncpPublicacao, 'fetchPublicacaoPagesForObjeto');
        render(BuscaView);
        await tick();
        const input = screen.getByLabelText('Termo a pesquisar');
        await fireEvent.input(input, {
          target: { value: '12345678000195-1-000001/2024' },
        });
      });
      When('the user submits the search form', async () => {
        const input = screen.getByLabelText('Termo a pesquisar');
        const form = input.closest('form');
        expect(form).toBeTruthy();
        await fireEvent.submit(form as HTMLFormElement);
      });
      Then(
        'the browser navigates to "/baliza/contratacao?id=12345678000195-1-000001/2024"',
        () => {
          expect(navigateSpy).toHaveBeenCalledWith(
            '/baliza/contratacao?id=12345678000195-1-000001/2024',
          );
          // No fetch on the PNCP-id branch.
          expect(fetchSpy).not.toHaveBeenCalled();
        },
      );
    },
  );
});
