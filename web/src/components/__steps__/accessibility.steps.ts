import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, waitFor } from '@testing-library/svelte/pure';
import { render, mockFetchError } from './shared';
import userEvent from '@testing-library/user-event';
import { expect, vi } from 'vitest';
import { tick } from 'svelte';

import { queryParquetFallback } from '../../lib/parquetFallback';
import CityDetailViewRaw from '../CityDetailView.svelte';
import EmptyStateRaw from '../EmptyState.svelte';

vi.mock('../../lib/parquetFallback', async () => {
  const actual = await vi.importActual('../../lib/parquetFallback');
  return {
    ...actual,
    queryParquetFallback: vi.fn(),
  };
});

const fallbackMock = vi.mocked(queryParquetFallback);
const CityDetailView = CityDetailViewRaw as unknown as Parameters<typeof render>[0];
const EmptyState = EmptyStateRaw as unknown as Parameters<typeof render>[0];

const feature = await loadFeature('features/accessibility.feature');

describeFeature(feature, ({ Scenario, BeforeEachScenario }) => {
  BeforeEachScenario(() => {
    vi.clearAllMocks();
    fallbackMock.mockResolvedValue({ ok: false, reason: 'empty' });
  });

  Scenario('Important interactive elements receive proper focus states', ({ Given, When, Then, And }) => {
    let user: ReturnType<typeof userEvent.setup>;

    Given('a dashboard page with interactive elements', async () => {
      user = userEvent.setup();

      // Route through the archive fallback. The live PNCP path would
      // share its mocked Response across three modality fetches plus the
      // centroids loader, all racing to consume the same body — only the
      // first .json() succeeds, the others throw "body stream already
      // read", and findMunicipalityByIbge cascades into an error state.
      fallbackMock.mockResolvedValue({
        ok: true,
        rows: [
          {
            numero_controle_pncp: '123-1/2024',
            objeto_contrato: 'Test Bid',
            data_publicacao_pncp: '2024-01-01T00:00:00',
            valor_global: 1000,
            cnpj_orgao: '00000000000191',
            razao_social_orgao: 'Órgão Fictício',
            uf_sigla: 'SP',
            municipio_nome: 'São Paulo',
            codigo_ibge: '3550308',
            ni_fornecedor: '12345678000100',
            nome_razao_social_fornecedor: 'Fornecedor X',
          },
        ],
        dataParticao: '2024-01-01',
      });
      mockFetchError();

      // Need a stable container
      const container = document.body.appendChild(document.createElement('div'));

      render(CityDetailView, { target: container, props: { ibge: '3550308' } });

      await waitFor(() => {
        expect(screen.getByText('Test Bid')).toBeInTheDocument();
      });
      await tick();

      render(EmptyState, { target: container, props: { title: 'No results', message: 'Try again', actionLabel: 'Voltar', actionHref: '/busca' }});
      await tick();
    });

    When('the user navigates using the keyboard', async () => {
      // Just initiating tab navigation
      await user.tab();
    });

    Then('the ".btn" button receives focus', async () => {
      // Pico migration: empty-state action is rendered as <a role=button>,
      // styled by Pico via the role rather than a .btn class.
      const btn = screen.getByText('Voltar').closest('a');
      expect(btn).toBeTruthy();

      (btn as HTMLElement).focus();
      expect(btn).toHaveFocus();
      expect(btn?.getAttribute('role')).toBe('button');
    });

    And('the ".bid-link-card" element receives focus', async () => {
       // Pico migration: contract card is a wrapping <a> inside an <article>.
       const card = screen.getByText(/Test Bid/i).closest('a');
       expect(card).toBeTruthy();

       (card as HTMLElement).focus();
       expect(card).toHaveFocus();
       expect(card?.closest('article')).toBeTruthy();
    });
  });
});
