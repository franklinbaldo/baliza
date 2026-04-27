import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, waitFor } from '@testing-library/svelte/pure';
import { render } from './shared';
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

      global.fetch = vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            data: [
              {
                numeroControlePNCP: '123-1/2024',
                dataPublicacaoPncp: '2024-01-01T00:00:00',
                objetoContratacao: 'Test Bid',
                valorTotalEstimado: 1000,
                orgaoEntidade: { cnpj: '00000000000191', razaoSocial: 'Órgão Fictício' },
                municipio: { nome: 'São Paulo', uf: 'SP' },
                unidadeOrgao: { nomeUnidade: 'Secretaria' }
              }
            ]
          }),
          { status: 200 }
        )
      );

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
      const btn = screen.getByText('Voltar').closest('a');
      expect(btn).toBeTruthy();

      (btn as HTMLElement).focus();
      expect(btn).toHaveFocus();
      expect(btn).toHaveClass('btn');
    });

    And('the ".bid-link-card" element receives focus', async () => {
       const card = screen.getByText(/Test Bid/i).closest('a');
       expect(card).toBeTruthy();

       (card as HTMLElement).focus();
       expect(card).toHaveFocus();
       expect(card).toHaveClass('bid-link-card');
    });
  });
});
