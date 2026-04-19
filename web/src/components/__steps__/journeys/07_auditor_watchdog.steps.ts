import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, fireEvent, waitFor } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { tick } from 'svelte';
import { render, noop, plannedStep } from './_shared';
import ContractDetailViewRaw from '../../ContractDetailView.svelte';
import * as parquetFallback from '../../../lib/parquetFallback';

const ContractDetailView = ContractDetailViewRaw as unknown as Parameters<typeof render>[0];

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
    Given('a search result list is visible for "dispensa acima de 1 milhão"', noop);
    When('the user clicks "Salvar vigilância"', noop);
    Then('a watch entry is persisted in localStorage', () => plannedStep('watch persistence'));
    And('the watch appears in the user\'s "Minhas vigilâncias" list', noop);
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
    Given('the user opens "/orgao?cnpj=00000000000191"', noop);
    When('the user clicks "Receber alertas deste órgão"', noop);
    Then('a watch is created with the agency CNPJ as the filter', () =>
      plannedStep('per-CNPJ subscribe-from-agency entry point'),
    );
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
