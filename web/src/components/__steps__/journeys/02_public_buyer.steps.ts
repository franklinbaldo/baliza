import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { noop, plannedStep, wipStep } from './_shared';

const feature = await loadFeature('features/journeys/02_public_buyer.feature');

describeFeature(feature, ({ Scenario }) => {
  Scenario('Browse vigent registered-price frameworks for an object', ({ Given, Then }) => {
    Given('the user opens "/atas?objeto=papel%20A4"', noop);
    Then(
      'the user sees a list of vigent atas with start date, end date, contracting agency and remaining quantity',
      () => plannedStep('atas browser route and vigent-frameworks data model'),
    );
  });

  Scenario(
    'Generate a price reference and export it as a citable PDF',
    ({ Given, When, Then, And }) => {
      Given('the user opens a market page for "papel A4"', noop);
      When('the user clicks "Gerar pesquisa de preços"', noop);
      Then('a PDF is produced containing min, average, median, max and standard deviation of unit price', () =>
        plannedStep('price-reference PDF generator'),
      );
      And('the PDF includes the source contract IDs and snapshot date', noop);
    },
  );

  Scenario(
    'Compare procurement practice across municipalities of similar size',
    ({ Given, Then, And }) => {
      Given('the user opens "/comparar?ibge=3550308&objeto=merenda"', noop);
      Then('the user sees three peer municipalities of similar population', () =>
        plannedStep('peer-municipality comparison route'),
      );
      And('the user sees the per-capita spend for the same object', noop);
    },
  );

  Scenario('Resolve a CATMAT or CATSER code from a free-text description', ({ Given, Then }) => {
    Given('the user types "papel sulfite branco A4 75g" into a catalog input', noop);
    Then('the user sees the most likely CATMAT codes ranked by match confidence', () =>
      plannedStep('CATMAT/CATSER catalog resolver'),
    );
  });

  Scenario('Inspect the legal basis cited by peers in similar exemptions', ({ Given, Then }) => {
    Given('the user opens "/dispensas?objeto=papel%20A4"', noop);
    Then('the user sees the most cited legal articles in similar dispensa contracts', () =>
      plannedStep('legal-basis aggregation across dispensa contracts'),
    );
  });

  Scenario(
    "Crossover with journey 3 — buyer audits a peer's contract before riding on it",
    ({ Given, Then, And }) => {
      Given('the user opens "/contratacao?id=00000000000191-1-000001/2024"', noop);
      Then("the user sees the contract's value, modality and supplier", () =>
        wipStep('buyer-friendly framing on the existing /contratacao page'),
      );
      And('the user sees an outbound link to the original PNCP record', noop);
    },
  );
});
