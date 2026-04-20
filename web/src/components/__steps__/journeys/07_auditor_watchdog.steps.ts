import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { noop, plannedStep } from './_shared';

const feature = await loadFeature('features/journeys/07_auditor_watchdog.feature');

describeFeature(feature, ({ Scenario }) => {
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
    Given('the user opens "/orgao?cnpj=00000000000191"', noop);
    When('the user clicks "Receber alertas deste órgão"', noop);
    Then('a watch is created with the agency CNPJ as the filter', () =>
      plannedStep('agency-page entry point for local watches'),
    );
  });

  Scenario(
    'Crossover with journey 4 — citizen turns a one-off search into a watch',
    ({ Given, When, Then }) => {
      Given('the user has just inspected a contract via /contratacao', noop);
      When('the user clicks "Acompanhar este fornecedor"', noop);
      Then('the user is offered a watch form pre-filled with the supplier CNPJ', () =>
        plannedStep('contract-page entry point for local watches'),
      );
    },
  );
});
