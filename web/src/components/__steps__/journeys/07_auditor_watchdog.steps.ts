import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { noop, plannedStep, render } from './_shared';
import { screen, fireEvent, waitFor, cleanup } from '@testing-library/svelte/pure';
import { expect, vi } from 'vitest';
import { tick } from 'svelte';
import AgencyDetailViewRaw from '../../AgencyDetailView.svelte';
import ContractDetailViewRaw from '../../ContractDetailView.svelte';
import { queryParquetFallback } from '../../../lib/parquetFallback';

vi.mock('../../../lib/parquetFallback', async () => {
  const actual = await vi.importActual('../../../lib/parquetFallback');
  return {
    ...actual,
    queryParquetFallback: vi.fn(),
  };
});

const AgencyDetailView = AgencyDetailViewRaw as unknown as Parameters<typeof render>[0];
const ContractDetailView = ContractDetailViewRaw as unknown as Parameters<typeof render>[0];

const fallbackMock = vi.mocked(queryParquetFallback);

const feature = await loadFeature('features/journeys/07_auditor_watchdog.feature');

describeFeature(feature, ({ Scenario, BeforeEachScenario }) => {
  BeforeEachScenario(() => {
    cleanup();
    vi.clearAllMocks();
    localStorage.clear();
    fallbackMock.mockResolvedValue({ ok: false, reason: 'empty' });
  });

  Scenario('Save the current query as a watch in localStorage', ({ Given, When, Then, And }) => {
    Given('a search result list is visible for "dispensa acima de 1 milhão"', noop);
    When('the user clicks "Salvar vigilância"', noop);
    Then('a watch entry is persisted in localStorage', () =>
      plannedStep('localStorage-backed watch persistence'),
    );
    And('the watch appears in the user\'s "Minhas vigilâncias" list', noop);
  });

  Scenario(
    'Curated RSS feed on Internet Archive publishes new matches',
    ({ Given, When, Then, And }) => {
      Given('a curated watch "dispensas-acima-1mi" is configured in the repo', noop);
      When(
        'the user opens "https://archive.org/download/baliza-pncp-feeds/feed-dispensas-acima-1mi.xml"',
        noop,
      );
      Then('the response is a valid RSS 2.0 document', () =>
        plannedStep('daily CI job that builds feed-{slug}.xml and uploads to IA'),
      );
      And('each item links to a /contratacao permalink', noop);
    },
  );

  Scenario('Diff view shows what changed since the last visit', ({ Given, When, Then }) => {
    Given('the user has previously visited a saved watch', noop);
    When('the user opens the watch again', noop);
    Then(
      'the user sees a "novidades desde sua última visita" section listing only new matches',
      () => plannedStep('client-side diff against last-visit snapshot in localStorage'),
    );
  });

  Scenario('Subscribe to a CNPJ from its agency page', ({ Given, When, Then }) => {
    Given('the user opens "/orgao?cnpj=00000000000191"', async () => {
      // Force the archive fallback path so the agency view is dataReady within
      // findByRole's 1s default. The bare-array live mock the original test
      // used races createListQuery's retry/backoff (queryClient.retry=1 with
      // 1s delay) and times out before the button renders.
      fallbackMock.mockResolvedValue({
        ok: true,
        rows: [
          {
            numero_controle_pncp: '00000000000191-1-000001/2024',
            objeto_contrato: 'Aquisição de teste',
            data_publicacao_pncp: '2024-01-01T00:00:00',
            valor_global: 1000,
            cnpj_orgao: '00000000000191',
            razao_social_orgao: 'Órgão Fictício',
            ni_fornecedor: '12345678000100',
            nome_razao_social_fornecedor: 'Fornecedor X',
          },
        ],
        dataParticao: '2024-01-01',
      });
      global.fetch = vi.fn().mockResolvedValue(new Response('Error', { status: 503 }));
      render(AgencyDetailView, { props: { cnpj: '00000000000191' } });
      await tick();
    });

    When('the user clicks "Receber alertas deste órgão"', async () => {
      const button = await screen.findByRole('button', { name: /Receber alertas deste órgão/i });
      await fireEvent.click(button);
    });

    Then('a watch is created with the agency CNPJ as the filter', async () => {
      await waitFor(() => {
        expect(screen.getByText('✓ Acompanhando')).toBeInTheDocument();
      });
      const stored = localStorage.getItem('baliza-watches');
      expect(stored).toBeTruthy();
      const watches = JSON.parse(stored!);
      expect(watches).toHaveLength(1);
      expect(watches[0].type).toBe('agency');
      expect(watches[0].filter).toBe('00000000000191');
    });
  });

  Scenario(
    'Crossover with journey 4 — citizen turns a one-off search into a watch',
    ({ Given, When, Then }) => {
      Given('the user has just inspected a contract via /contratacao', async () => {
        fallbackMock.mockResolvedValue({
          ok: true,
          rows: [
            {
              numero_controle_pncp: '12345678000195-1-000001/2024',
              objeto_contrato: 'Something',
              data_publicacao_pncp: '2024-01-01T00:00:00',
              valor_global: 1000,
              cnpj_orgao: '12345678000195',
              razao_social_orgao: 'Buyer',
              ni_fornecedor: '98765432000111',
              nome_razao_social_fornecedor: 'Supplier S.A.',
            },
          ],
          dataParticao: '2024-01-01',
        });

        global.fetch = vi.fn().mockResolvedValue(new Response('Error', { status: 503 }));

        render(ContractDetailView, { props: { id: '12345678000195-1-000001/2024' } });
        await tick();
      });

      When('the user clicks "Acompanhar este fornecedor"', async () => {
        const button = await screen.findByRole('button', { name: /Acompanhar este fornecedor/i });
        await fireEvent.click(button);
      });

      Then('the user is offered a watch form pre-filled with the supplier CNPJ', async () => {
        await waitFor(() => {
          expect(screen.getByText('✓ Acompanhando')).toBeInTheDocument();
        });
        const stored = localStorage.getItem('baliza-watches');
        expect(stored).toBeTruthy();
        const watches = JSON.parse(stored!);
        const watch = watches[watches.length - 1];
        expect(watch.type).toBe('supplier');
        expect(watch.filter).toBe('98765432000111');
      });
    },
  );
});
