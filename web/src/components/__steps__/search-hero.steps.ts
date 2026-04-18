import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, fireEvent, waitFor } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
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

function stubLocationAssign() {
  const assign = vi.fn();
  Object.defineProperty(window, 'location', {
    configurable: true,
    value: { ...window.location, assign },
  });
  return assign;
}

describeFeature(feature, ({ Scenario, BeforeEachScenario }) => {
  BeforeEachScenario(async () => {
    cleanup();
    vi.restoreAllMocks();
    vi.useRealTimers();
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

  Scenario('Submitting a CNPJ navigates to the agency page', ({ Given, When, Then }) => {
    let assign: ReturnType<typeof vi.fn>;

    Given('the user types "12345678000195" into the search box', async () => {
      assign = stubLocationAssign();
      render(SearchHero);
      await tick();
      await typeInSearch('12345678000195');
    });

    When('the user submits the search form', async () => {
      const form = document.querySelector('form.search-form') as HTMLFormElement;
      await fireEvent.submit(form);
      await tick();
    });

    Then('the browser should navigate to "/baliza/orgao?cnpj=12345678000195"', () => {
      expect(assign).toHaveBeenCalledWith('/baliza/orgao?cnpj=12345678000195');
    });
  });

  Scenario('Submitting an IBGE code navigates to the municipality page', ({ Given, When, Then }) => {
    let assign: ReturnType<typeof vi.fn>;

    Given('the user types "3550308" into the search box', async () => {
      assign = stubLocationAssign();
      render(SearchHero);
      await tick();
      await typeInSearch('3550308');
    });

    When('the user submits the search form', async () => {
      const form = document.querySelector('form.search-form') as HTMLFormElement;
      await fireEvent.submit(form);
      await tick();
    });

    Then('the browser should navigate to "/baliza/municipio?ibge=3550308"', () => {
      expect(assign).toHaveBeenCalledWith('/baliza/municipio?ibge=3550308');
    });
  });

  Scenario('Submitting a canonical PNCP contract ID navigates to the contract page', ({ Given, When, Then }) => {
    let assign: ReturnType<typeof vi.fn>;

    Given('the user types "12345678000195-1-000001/2024" into the search box', async () => {
      assign = stubLocationAssign();
      render(SearchHero);
      await tick();
      await typeInSearch('12345678000195-1-000001/2024');
    });

    When('the user submits the search form', async () => {
      const form = document.querySelector('form.search-form') as HTMLFormElement;
      await fireEvent.submit(form);
      await tick();
    });

    Then('the browser should navigate to "/baliza/contratacao?id=12345678000195-1-000001/2024"', () => {
      expect(assign).toHaveBeenCalledWith('/baliza/contratacao?id=12345678000195-1-000001/2024');
    });
  });

  Scenario('Reject PNCP ID with missing year segment', ({ Given, Then }) => {
    Given('the user types "12345678000195-1-000001" into the search box', async () => {
      render(SearchHero);
      await tick();
      await typeInSearch('12345678000195-1-000001');
    });

    Then('I should not see any jump suggestion', () => {
      expect(screen.queryByText(/Ver Contratação/)).toBeNull();
    });
  });

  Scenario('Reject PNCP ID with 3-digit year', ({ Given, Then }) => {
    Given('the user types "12345678000195-1-000001/202" into the search box', async () => {
      render(SearchHero);
      await tick();
      await typeInSearch('12345678000195-1-000001/202');
    });

    Then('I should not see any jump suggestion', () => {
      expect(screen.queryByText(/Ver Contratação/)).toBeNull();
    });
  });

  Scenario('Reject hyphen-only PNCP ID', ({ Given, Then }) => {
    Given('the user types "12345678000195-1-000001-1" into the search box', async () => {
      render(SearchHero);
      await tick();
      await typeInSearch('12345678000195-1-000001-1');
    });

    Then('I should not see any jump suggestion', () => {
      expect(screen.queryByText(/Ver Contratação/)).toBeNull();
    });
  });

  Scenario('Free text triggers a PNCP search and renders results listbox', ({ Given, When, Then, And }) => {
    Given('the PNCP search API returns two results for "hospital"', () => {
      const payload = {
        items: [
          {
            numero_controle_pncp: '00000000000191-1-000001/2024',
            title: 'Edital nº 001/2024',
            description: 'Aquisição de materiais hospitalares para atendimento emergencial',
            orgao_nome: 'Secretaria Municipal de Saúde',
            data_publicacao_pncp: '2024-05-01T00:00:00',
            valor_global: 150000,
          },
          {
            numero_controle_pncp: '00000000000191-1-000002/2024',
            title: 'Edital nº 002/2024',
            description: 'Contrato de manutenção de equipamento hospitalar',
            orgao_nome: 'Fundo de Saúde',
            data_publicacao_pncp: '2024-04-12T00:00:00',
            valor_global: 50000,
          },
        ],
        total: 2,
      };
      global.fetch = vi
        .fn()
        .mockImplementation(async () => new Response(JSON.stringify(payload), { status: 200 }));
    });

    When('the user types "hospital" into the search box', async () => {
      render(SearchHero);
      await tick();
      await typeInSearch('hospital');
    });

    Then('I should see a results listbox with at least one link', async () => {
      await waitFor(
        () => {
          const listbox = screen.getByRole('listbox');
          expect(listbox).toBeTruthy();
          expect(listbox.querySelectorAll('a').length).toBeGreaterThan(0);
        },
        { timeout: 2000 },
      );
    });

    And('the first result should link to a contratação page', () => {
      const firstLink = screen.getByRole('listbox').querySelector('a') as HTMLAnchorElement;
      expect(firstLink.getAttribute('href')).toMatch(/\/baliza\/contratacao\?id=/);
    });
  });

  Scenario('PNCP search failure shows an alert banner', ({ Given, When, Then }) => {
    Given('the PNCP search API fails for "hospital"', () => {
      global.fetch = vi
        .fn()
        .mockImplementation(async () => new Response('boom', { status: 500 }));
    });

    When('the user types "hospital" into the search box', async () => {
      render(SearchHero);
      await tick();
      await typeInSearch('hospital');
    });

    Then('I should see an alert banner about the search error', async () => {
      await waitFor(
        () => {
          const alert = screen.getByRole('alert');
          expect(alert.textContent).toMatch(/busca|PNCP/i);
        },
        { timeout: 2000 },
      );
    });
  });

  Scenario('PNCP search zero results shows an empty state', ({ Given, When, Then }) => {
    Given('the PNCP search API returns zero results for "xyzneverexists"', () => {
      global.fetch = vi
        .fn()
        .mockImplementation(
          async () => new Response(JSON.stringify({ items: [], total: 0 }), { status: 200 }),
        );
    });

    When('the user types "xyzneverexists" into the search box', async () => {
      render(SearchHero);
      await tick();
      await typeInSearch('xyzneverexists');
    });

    Then('I should see an empty state for the search', async () => {
      await waitFor(
        () => {
          expect(screen.getByText(/Nenhum resultado/i)).toBeTruthy();
        },
        { timeout: 2000 },
      );
    });
  });
});
