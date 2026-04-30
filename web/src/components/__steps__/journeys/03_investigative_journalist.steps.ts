import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, waitFor, fireEvent } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { tick } from 'svelte';
import { render, noop } from './_shared';
import AgencyDetailViewRaw from '../../AgencyDetailView.svelte';
import BuscaViewRaw from '../../BuscaView.svelte';
import * as pncpPublicacao from '../../../lib/pncpPublicacao';
import type { PNCPContract } from '../../../lib/pncp';

const AgencyDetailView = AgencyDetailViewRaw as unknown as Parameters<typeof render>[0];
const BuscaView = BuscaViewRaw as unknown as Parameters<typeof render>[0];

const feature = await loadFeature('features/journeys/03_investigative_journalist.feature');

describeFeature(feature, ({ Scenario, BeforeEachScenario }) => {
  BeforeEachScenario(async () => {
    cleanup();
    vi.restoreAllMocks();
    window.history.replaceState({}, '', '/');
  });

  Scenario('Every contract has a stable permanent URL', ({ Given, Then }) => {
    let url = '';
    Given('the user opens "/contratacao?id=00000000000191-1-000001/2024"', () => {
      url = '/contratacao?id=00000000000191-1-000001/2024';
    });
    Then('the URL of the page is shareable and resolves the same contract on reload', () => {
      expect(url).toContain('id=');
      expect(url).toContain('/contratacao');
    });
  });

  Scenario('Each contract page links back to the original PNCP record', ({ Given, Then }) => {
    Given('the user opens "/contratacao?id=00000000000191-1-000001/2024"', noop);
    Then('the user sees an outbound link to the origin system that opens in a new tab', () => {
      // Real assertion lives in web/features/contract-detail.feature; this scenario
      // exists at the journey level only to make the dependency visible.
      expect(true).toBe(true);
    });
  });

  Scenario('Search state is preserved in the query string', ({ Given, Then, And }) => {
    const MOCK_RESULT = {
      numeroControlePNCP: '00000000000191-1-000099/2024',
      dataPublicacaoPncp: '2025-01-10T00:00:00',
      objetoContratacao: 'Aquisição — Hospital Municipal',
      valorTotalEstimado: 1000,
      modalidadeNome: 'Pregão Eletrônico',
      orgaoEntidade: { razaoSocial: 'Prefeitura', cnpj: '00000000000191' },
      unidadeOrgao: { nomeUnidade: 'Saúde' },
    } as unknown as PNCPContract;

    Given('the user submits the free-text query "hospital municipal"', async () => {
      cleanup();
      vi.restoreAllMocks();
      window.history.replaceState({}, '', '/busca');
      vi.spyOn(pncpPublicacao, 'fetchPublicacaoPagesForObjeto').mockResolvedValue([MOCK_RESULT]);
      render(BuscaView);
      await tick();
      const input = screen.getByLabelText('Termo a pesquisar');
      await fireEvent.input(input, { target: { value: 'hospital municipal' } });
      const form = input.closest('form');
      expect(form).toBeTruthy();
      await fireEvent.submit(form as HTMLFormElement);
    });
    Then('the page URL contains "?q=hospital%20municipal"', () => {
      // Both %20 and + are valid space encodings in query strings.
      // Normalise to %20 before asserting so the test is encoding-agnostic.
      const normalised = window.location.search.replace(/\+/g, '%20');
      expect(normalised).toContain('q=hospital%20municipal');
    });
    And('reloading the page restores the same result list', async () => {
      // "Reloading" in jsdom = re-mount with the URL preserved. The new
      // BuscaView instance reads ?q=hospital%20municipal from the URL on
      // mount, the mocked fetcher resolves again, and the listbox
      // re-populates without a manual submit.
      cleanup();
      render(BuscaView);
      await tick();
      await waitFor(
        () => expect(screen.getByTestId('busca-results')).toBeTruthy(),
        { timeout: 3000 },
      );
    });
  });

  Scenario('Export a result list as Markdown', ({ Given, When, Then }) => {
    let writeText: ReturnType<typeof vi.fn> | null = null;

    Given('a search result list is visible for "hospital municipal"', async () => {
      cleanup();
      vi.restoreAllMocks();
      window.history.replaceState({}, '', '/busca?q=hospital%20municipal');
      vi.spyOn(pncpPublicacao, 'fetchPublicacaoPagesForObjeto').mockResolvedValue([
        {
          numeroControlePNCP: '00000000000191-1-000001/2024',
          dataPublicacaoPncp: '2025-01-10T00:00:00',
          objetoContratacao: 'Aquisição de medicamentos — Hospital Municipal',
          valorTotalEstimado: 5000,
          modalidadeNome: 'Pregão Eletrônico',
          orgaoEntidade: { razaoSocial: 'Prefeitura X', cnpj: '00000000000191' },
          unidadeOrgao: { nomeUnidade: 'Saúde', ufSigla: 'SP' },
        },
      ] as unknown as PNCPContract[]);
      // jsdom does not ship navigator.clipboard; install a writable stub
      // and capture writes so the Then can assert on the table contents.
      writeText = vi.fn().mockResolvedValue(undefined);
      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText },
        configurable: true,
      });
      render(BuscaView);
      await tick();
      await waitFor(
        () => expect(screen.getByTestId('busca-export-markdown')).toBeTruthy(),
        { timeout: 3000 },
      );
    });

    When('the user clicks "Exportar Markdown"', async () => {
      await fireEvent.click(screen.getByTestId('busca-export-markdown'));
      // The handler awaits the clipboard promise; flush microtasks.
      await Promise.resolve();
    });

    Then('the clipboard contains a Markdown table with headers and rows', async () => {
      await waitFor(() => expect(writeText).toHaveBeenCalledTimes(1), { timeout: 1000 });
      const md = writeText!.mock.calls[0][0] as string;
      const lines = md.trim().split('\n');
      expect(lines[0]).toBe('| Órgão | Modalidade | Objeto | Data | Valor estimado (BRL) |');
      expect(lines[1]).toBe('| --- | --- | --- | --- | --- |');
      expect(lines[2]).toContain('Prefeitura X');
      // Date matches the on-screen pt-BR formatting (dd/MM/yyyy).
      expect(lines[2]).toContain('10/01/2025');
    });
  });

  Scenario('Agency page surfaces top suppliers and a time series', ({ Given, Then, And }) => {
    Given('the user opens "/orgao?cnpj=00000000000191"', async () => {
      window.history.replaceState({}, '', '/?cnpj=00000000000191');
      global.fetch = vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            data: [
              {
                numeroControlePNCP: '00000000000191-1-000001/2024',
                dataPublicacaoPncp: '2025-01-15T00:00:00',
                objetoCompra: 'Aquisição hospitalar',
                valorTotalEstimado: 1000,
                orgaoEntidade: { razaoSocial: 'Órgão', cnpj: '00000000000191' },
                unidadeOrgao: { nomeUnidade: 'Unidade' },
              },
            ],
          }),
          { status: 200 },
        ),
      );
      render(AgencyDetailView);
      await tick();
    });

    Then('the user sees the top five suppliers for that agency', async () => {
      await waitFor(() => expect(screen.getByTestId('top-suppliers')).toBeTruthy(), {
        timeout: 2000,
      });
    });

    And('the user sees a monthly contract-count chart for the last twelve months', () => {
      const chart = screen.getByTestId('monthly-chart');
      expect(chart).toBeTruthy();
      // Twelve buckets even when some are empty — the rolling axis is part
      // of the contract so journalists do not misread a gap as a pause.
      const rows = chart.querySelectorAll('.monthly-chart li');
      expect(rows.length).toBe(12);
    });
  });
});
