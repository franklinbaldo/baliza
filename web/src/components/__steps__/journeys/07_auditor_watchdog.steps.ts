import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { noop, plannedStep, wipStep } from './_shared';

const feature = await loadFeature('features/journeys/07_auditor_watchdog.feature');

describeFeature(feature, ({ Scenario }) => {
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
      Given('the user has just inspected a contract via /contratacao', noop);
      When('the user clicks "Acompanhar este fornecedor"', noop);
      Then('the user is offered a watch form pre-filled with the supplier CNPJ', () =>
        wipStep('"Acompanhar este fornecedor" entry point on contract page'),
      );
    },
  );
});
