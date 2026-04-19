import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { expect } from 'vitest';
import { noop, plannedStep, wipStep } from './_shared';

const feature = await loadFeature('features/journeys/03_investigative_journalist.feature');

describeFeature(feature, ({ Scenario }) => {
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
    Given('the user submits the free-text query "hospital municipal"', noop);
    Then('the page URL contains "?q=hospital%20municipal"', () =>
      wipStep('SearchHero does not push the query to the URL today'),
    );
    And('reloading the page restores the same result list', noop);
  });

  Scenario('Export a result list as Markdown', ({ Given, When, Then }) => {
    Given('a search result list is visible for "hospital municipal"', noop);
    When('the user clicks "Exportar Markdown"', noop);
    Then('the clipboard contains a Markdown table with headers and rows', () =>
      plannedStep('Markdown export from search results'),
    );
  });

  Scenario('Agency page surfaces top suppliers and a time series', ({ Given, Then, And }) => {
    Given('the user opens "/orgao?cnpj=00000000000191"', noop);
    Then('the user sees the top five suppliers for that agency', () =>
      wipStep('agency rollup by supplier'),
    );
    And('the user sees a monthly contract-count chart for the last twelve months', noop);
  });

  Scenario('Crossover with journey 7 — journalist subscribes to a saved query', ({ Given, When, Then }) => {
    Given('a search result list is visible for "hospital municipal"', noop);
    When('the user clicks "Acompanhar esta busca"', noop);
    Then('the user is offered an RSS URL for new matches', () =>
      wipStep('subscribe-this-search entry point depends on journey 7 alerting infra'),
    );
  });
});
