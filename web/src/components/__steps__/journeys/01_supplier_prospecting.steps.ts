import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { noop, plannedStep, wipStep } from './_shared';

const feature = await loadFeature('features/journeys/01_supplier_prospecting.feature');

describeFeature(feature, ({ Scenario }) => {
  Scenario('Search for an object opens an aggregated market page', ({ Given, When, Then, And }) => {
    Given('the user opens the home page', noop);
    When('the user submits the free-text query "merenda escolar"', noop);
    Then('the user lands on a market page summarizing top buyers, top suppliers and price ranges', () =>
      plannedStep('market aggregation page (/mercado/{objeto})'),
    );
    And('the page shows the number of distinct contracts found', noop);
  });

  Scenario('Filter contracts by UF and modality recomputes aggregates', ({ Given, When, Then }) => {
    Given('a search result list is visible', noop);
    When('the user picks UF "SP" and modality "Pregão Eletrônico"', noop);
    Then('the visible aggregates (count, total value, average value) reflect the filtered subset', () =>
      wipStep('UF + modality aggregate filter on search results'),
    );
  });

  Scenario('Supplier page by CNPJ shows history, peers and average ticket', ({ Given, Then, And }) => {
    Given('the user opens "/fornecedor?cnpj=12345678000195"', noop);
    Then("the user sees the supplier's contract history", () =>
      plannedStep('/fornecedor?cnpj= route and supplier rollup view'),
    );
    And("the user sees the supplier's top three competing CNPJs for the same objects", noop);
    And("the user sees the supplier's average ticket size", noop);
  });

  Scenario('Export the current search result as CSV', ({ Given, When, Then }) => {
    Given('a search result list is visible for "merenda escolar"', noop);
    When('the user clicks "Exportar CSV"', noop);
    Then('a CSV file is downloaded with named columns matching the visible table', () =>
      wipStep('CSV export from search result list (only explorer can export today)'),
    );
  });

  Scenario('Empty search suggests accent-tolerant alternatives', ({ Given, When, Then }) => {
    Given('the user submits the free-text query "construcao de escola"', noop);
    When('PNCP returns zero results', noop);
    Then('the user sees a suggestion to retry with "construção de escola"', () =>
      wipStep('accent-tolerant search suggestion in empty state'),
    );
  });

  Scenario(
    'Crossover with journey 2 — supplier inspects the buyer\'s pricing reference',
    ({ Given, When, Then }) => {
      Given('a market page for "merenda escolar" is visible', noop);
      When('the user clicks "Ver pesquisa de preços"', noop);
      Then('the user sees the same evidenced price reference shown to public buyers', () =>
        plannedStep('shared price-reference module across market and buyer pages'),
      );
    },
  );
});
