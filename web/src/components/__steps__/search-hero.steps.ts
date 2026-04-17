import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, fireEvent, waitFor } from '@testing-library/svelte/pure';
import { expect } from 'vitest';
import { tick } from 'svelte';
import { render } from './shared';
import SearchHeroRaw from '../SearchHero.svelte';

const SearchHero = SearchHeroRaw as unknown as Parameters<typeof render>[0];
const feature = await loadFeature('features/search-hero.feature');

async function typeInSearch(value: string) {
  const input = screen.getByLabelText('Buscar contratos públicos') as HTMLInputElement;
  await fireEvent.input(input, { target: { value } });
  await tick();
  return input;
}

describeFeature(feature, ({ Scenario, BeforeEachScenario }) => {
  BeforeEachScenario(async () => {
    cleanup();
  });

  Scenario('Detect CNPJ pattern and show correct jump link', ({ Given, Then, And }) => {
    Given('the user types "12345678000195" into the search box', async () => {
      render(SearchHero);
      await tick();
      await typeInSearch('12345678000195');
    });

    Then('I should see "Explorar Órgão" as a jump suggestion', async () => {
      await waitFor(() => expect(screen.getByText(/Explorar Órgão/)).toBeTruthy());
    });

    And('the link should point to "/baliza/orgao?cnpj=12345678000195"', () => {
      const link = screen.getByRole('link', { name: /Explorar Órgão/ });
      expect(link.getAttribute('href')).toBe('/baliza/orgao?cnpj=12345678000195');
    });
  });

  Scenario('Detect IBGE code and show correct jump link', ({ Given, Then, And }) => {
    Given('the user types "3550308" into the search box', async () => {
      render(SearchHero);
      await tick();
      await typeInSearch('3550308');
    });

    Then('I should see "Explorar Município" as a jump suggestion', async () => {
      await waitFor(() => expect(screen.getByText(/Explorar Município/)).toBeTruthy());
    });

    And('the link should point to "/baliza/municipio?ibge=3550308"', () => {
      const link = screen.getByRole('link', { name: /Explorar Município/ });
      expect(link.getAttribute('href')).toBe('/baliza/municipio?ibge=3550308');
    });
  });

  Scenario('Detect PNCP ID pattern and show jump link', ({ Given, Then }) => {
    Given('the user types "12345678000195-1-000001/2024" into the search box', async () => {
      render(SearchHero);
      await tick();
      await typeInSearch('12345678000195-1-000001/2024');
    });

    Then('I should see "Ver Contratação" as a jump suggestion', async () => {
      await waitFor(() => expect(screen.getByText(/Ver Contratação/)).toBeTruthy());
    });
  });

  Scenario('Short query shows no jump suggestion', ({ Given, Then }) => {
    Given('the user types "ab" into the search box', async () => {
      render(SearchHero);
      await tick();
      await typeInSearch('ab');
    });

    Then('I should not see any jump suggestion', async () => {
      await waitFor(() => {
        expect(screen.queryByText(/Explorar Órgão/)).toBeNull();
        expect(screen.queryByText(/Explorar Município/)).toBeNull();
        expect(screen.queryByText(/Ver Contratação/)).toBeNull();
      });
    });
  });

  Scenario('Hint chips fill the input', ({ Given, When, Then }) => {
    Given('the search hero has loaded', async () => {
      render(SearchHero);
      await tick();
    });

    When('the user clicks the first hint chip', async () => {
      const chips = document.querySelectorAll('.hint-chip');
      if (chips.length > 0) await fireEvent.click(chips[0]);
      await tick();
    });

    Then('the input value should not be empty', () => {
      const input = screen.getByLabelText('Buscar contratos públicos') as HTMLInputElement;
      expect(input.value.length).toBeGreaterThan(0);
    });
  });

  Scenario('Search input has accessible label', ({ When, Then }) => {
    When('the search hero loads', async () => {
      render(SearchHero);
      await tick();
    });

    Then('the input should have an accessible label', () => {
      expect(screen.getByLabelText('Buscar contratos públicos')).toBeTruthy();
    });
  });
});
