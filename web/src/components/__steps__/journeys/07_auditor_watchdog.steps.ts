import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, fireEvent, waitFor } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { tick } from 'svelte';
import { render, noop, plannedStep } from './_shared';
import ContractDetailViewRaw from '../../ContractDetailView.svelte';
import SearchHeroRaw from '../../SearchHero.svelte';
import AgencyDetailViewRaw from '../../AgencyDetailView.svelte';
import WatchListRaw from '../../WatchList.svelte';
import * as parquetFallback from '../../../lib/parquetFallback';

const ContractDetailView = ContractDetailViewRaw as unknown as Parameters<typeof render>[0];
const SearchHero = SearchHeroRaw as unknown as Parameters<typeof render>[0];
const AgencyDetailView = AgencyDetailViewRaw as unknown as Parameters<typeof render>[0];
const WatchList = WatchListRaw as unknown as Parameters<typeof render>[0];

const feature = await loadFeature('features/journeys/07_auditor_watchdog.feature');

const ARCHIVED_ROW = {
  numero_controle_pncp: '00000000000191-1-000001/2024',
  data_publicacao_pncp: '2025-01-15',
  objeto_contrato: 'Prestação de serviços',
  valor_global: 1500,
  valor_inicial: 1500,
  modalidade_nome: 'Dispensa',
  link_sistema_origem: 'https://origem.exemplo.gov.br/compras/1',
  cnpj_orgao: '00000000000191',
  razao_social_orgao: 'Prefeitura',
  nome_unidade: 'Compras',
  ni_fornecedor: '12345678000195',
  nome_razao_social_fornecedor: 'Fornecedora Exemplo Ltda',
};

describeFeature(feature, ({ Scenario, BeforeEachScenario }) => {
  BeforeEachScenario(async () => {
    cleanup();
    vi.restoreAllMocks();
    window.history.replaceState({}, '', '/');
    try {
      window.localStorage.removeItem('baliza.watches');
    } catch {
      // no-op in environments without storage
    }
  });

  Scenario('Save the current query as a watch in localStorage', ({ Given, When, Then, And }) => {
    const SEARCH_PAYLOAD = {
      items: [
        {
          numero_controle_pncp: '00000000000191-1-000001/2024',
          description: 'Dispensa de licitação acima de 1 milhão',
          orgao_nome: 'Prefeitura',
          data_publicacao_pncp: '2024-03-01T00:00:00',
          valor_global: 1_200_000,
          uf_sigla: 'SP',
          modalidade_nome: 'Dispensa',
        },
      ],
      total: 1,
    };

    Given('a search result list is visible for "dispensa acima de 1 milhão"', async () => {
      global.fetch = vi
        .fn()
        .mockImplementation(async () => new Response(JSON.stringify(SEARCH_PAYLOAD), { status: 200 }));
      render(SearchHero);
      await tick();
      const input = screen.getByLabelText('Buscar contratos públicos') as HTMLInputElement;
      await fireEvent.input(input, { target: { value: 'dispensa acima de 1 milhão' } });
      await tick();
      await waitFor(() => expect(screen.getByRole('listbox')).toBeTruthy(), { timeout: 2000 });
    });

    When('the user clicks "Salvar vigilância"', async () => {
      const btn = screen.getByTestId('save-watch');
      await fireEvent.click(btn);
      await tick();
    });

    Then('a watch entry is persisted in localStorage', () => {
      const raw = window.localStorage.getItem('baliza.watches');
      expect(raw).toBeTruthy();
      const parsed = JSON.parse(raw!);
      expect(Array.isArray(parsed)).toBe(true);
      expect(parsed.length).toBe(1);
      expect(parsed[0].kind).toBe('query');
      expect(parsed[0].term).toContain('dispensa');
    });

    And('the watch appears in the user\'s "Minhas vigilâncias" list', async () => {
      // Render the list component separately; it reads the same localStorage
      // key that the save step just populated.
      cleanup();
      render(WatchList);
      await tick();
      await waitFor(() => expect(screen.getByTestId('my-watches')).toBeTruthy());
      const items = screen.getAllByTestId(/^watch-item-/);
      expect(items.length).toBe(1);
    });
  });

  Scenario('RSS feed publishes new matches for a saved watch', ({ Given, When, Then, And }) => {
    Given('a watch named "dispensas-acima-1mi" exists', noop);
    When('the user opens "/alertas/dispensas-acima-1mi.xml"', noop);
    Then('the response is a valid RSS 2.0 document', () => plannedStep('/alertas/{slug}.xml RSS route'));
    And('each item links to a /contratacao permalink', noop);
  });

  Scenario('Webhook fires on new matches via a stateless GitHub Action', ({ Given, When, Then, And }) => {
    Given('a watch with webhook URL "https://example.org/hook" exists', noop);
    When('a daily extraction adds a contract that matches the watch', noop);
    Then('the GitHub Action posts a JSON payload to the webhook URL', () =>
      plannedStep('watch webhook dispatcher GitHub Action'),
    );
    And("the payload contains the matching contract's PNCP ID", noop);
  });

  Scenario('Diff view shows what changed since the last visit', ({ Given, When, Then }) => {
    Given('the user has previously visited a saved watch', noop);
    When('the user opens the watch again', noop);
    Then('the user sees a "novidades desde sua última visita" section listing only new matches', () =>
      plannedStep('visit-diff view for saved watches'),
    );
  });

  Scenario('Subscribe to a CNPJ from its agency or supplier page', ({ Given, When, Then }) => {
    Given('the user opens "/orgao?cnpj=00000000000191"', async () => {
      window.history.replaceState({}, '', '/?cnpj=00000000000191');
      global.fetch = vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            data: [
              {
                numeroControlePNCP: '00000000000191-1-000001/2024',
                dataPublicacaoPncp: '2025-01-15T00:00:00',
                objetoContratacao: 'Contratação',
                valorTotalEstimado: 1000,
                orgaoEntidade: { razaoSocial: 'Prefeitura Municipal', cnpj: '00000000000191' },
                unidadeOrgao: { nomeUnidade: 'Compras' },
              },
            ],
          }),
          { status: 200 },
        ),
      );
      render(AgencyDetailView);
      await tick();
      await waitFor(() => expect(screen.getByTestId('subscribe-agency')).toBeTruthy(), {
        timeout: 2000,
      });
    });

    When('the user clicks "Receber alertas deste órgão"', async () => {
      const btn = screen.getByTestId('subscribe-agency');
      expect(btn.textContent).toContain('Receber alertas deste órgão');
      await fireEvent.click(btn);
      await tick();
    });

    Then('a watch is created with the agency CNPJ as the filter', () => {
      const raw = window.localStorage.getItem('baliza.watches');
      expect(raw).toBeTruthy();
      const parsed = JSON.parse(raw!);
      expect(parsed.length).toBe(1);
      expect(parsed[0].kind).toBe('cnpj-agency');
      expect(parsed[0].cnpj).toBe('00000000000191');
    });
  });

  Scenario(
    'Crossover with journey 4 — citizen turns a one-off search into a watch',
    ({ Given, When, Then }) => {
      Given('the user has just inspected a contract via /contratacao', async () => {
        window.history.replaceState({}, '', '/?id=00000000000191-1-000001/2024');
        // Drive the archive path so the contract view model has a supplier
        // CNPJ — the live PNCP publicacao endpoint does not expose it.
        vi.spyOn(parquetFallback, 'queryParquetFallback').mockResolvedValue({
          ok: true,
          rows: [ARCHIVED_ROW],
          dataParticao: '2025-01-31',
        });
        global.fetch = vi
          .fn()
          .mockResolvedValue(new Response('boom', { status: 503 }));
        render(ContractDetailView);
        await tick();
      });

      When('the user clicks "Acompanhar este fornecedor"', async () => {
        await waitFor(() => expect(screen.getByTestId('watch-supplier-card')).toBeTruthy(), {
          timeout: 2000,
        });
        const btn = screen.getByTestId('open-watch-form') as HTMLButtonElement;
        await fireEvent.click(btn);
        await tick();
      });

      Then('the user is offered a watch form pre-filled with the supplier CNPJ', async () => {
        await waitFor(() => {
          const form = screen.getByTestId('watch-form');
          expect(form).toBeTruthy();
          const cnpjInput = screen.getByTestId('watch-cnpj') as HTMLInputElement;
          expect(cnpjInput.value).toBe('12345678000195');
          expect(cnpjInput.readOnly).toBe(true);
        });
      });
    },
  );
});
