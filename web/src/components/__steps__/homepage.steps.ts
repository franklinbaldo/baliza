import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, waitFor } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { tick } from 'svelte';
import { render } from './shared';
import DashboardRaw from '../Dashboard.svelte';

const Dashboard = DashboardRaw as unknown as Parameters<typeof render>[0];
const feature = await loadFeature('features/homepage.feature');

const validStats = { total_contracts: 99000, total_quarantine: 1, days_on_ia: 365 };
const heroStats = { total_contracts: 5804, total_quarantine: 3, days_on_ia: 3 };

describeFeature(feature, ({ Scenario, BeforeEachScenario }) => {
  BeforeEachScenario(async () => {
    cleanup();
    vi.restoreAllMocks();
  });

  Scenario('Show skeleton loading state before stats arrive', ({ Given, When, Then }) => {
    Given('the stats fetch is still pending', async () => {
      global.fetch = vi.fn().mockReturnValue(new Promise(() => {}));
      render(Dashboard);
      await tick();
    });

    When('the dashboard mounts', () => {});

    Then('I should see skeleton placeholder cards', async () => {
      await waitFor(() => {
        expect(document.querySelector('[aria-busy="true"]')).toBeTruthy();
      });
    });
  });

  Scenario('Render stats after successful fetch', ({ Given, When, Then }) => {
    Given('the fetch returns total_contracts 99000', async () => {
      global.fetch = vi.fn().mockResolvedValue(
        new Response(JSON.stringify(validStats), { status: 200 }),
      );
      render(Dashboard);
      await tick();
    });

    When('the dashboard mounts', () => {});

    Then('I should see "99.000" in the stats', async () => {
      await waitFor(() => expect(screen.getByText('99.000')).toBeTruthy());
    });
  });

  Scenario('KPI numbers render with Brazilian thousand separator', ({ Given, When, Then }) => {
    Given('the fetch returns total_contracts 5804', async () => {
      global.fetch = vi.fn().mockResolvedValue(
        new Response(JSON.stringify(heroStats), { status: 200 }),
      );
      render(Dashboard);
      await tick();
    });

    When('the dashboard mounts', () => {});

    Then('I should see "5.804" in the stats', async () => {
      await waitFor(() => expect(screen.getByText('5.804')).toBeTruthy());
    });
  });

  Scenario('Quarantine KPI links to the glossary anchor', ({ Given, When, Then }) => {
    Given('the fetch returns valid stats', async () => {
      global.fetch = vi.fn().mockResolvedValue(
        new Response(JSON.stringify(validStats), { status: 200 }),
      );
      render(Dashboard);
      await tick();
      await waitFor(() => screen.getByText('99.000'));
    });

    When('the dashboard has finished loading', () => {});

    Then('the "Em quarentena" card should link to "/baliza/sobre#quarentena"', () => {
      const link = screen.getByRole('link', { name: /em quarentena/i });
      expect(link.getAttribute('href')).toBe('/sobre#quarentena');
    });
  });

  Scenario('Show error banner when stats fail to load', ({ Given, When, Then, And }) => {
    Given('the stats fetch fails with "Falha na rede"', async () => {
      global.fetch = vi.fn().mockRejectedValue(new Error('Falha na rede'));
      render(Dashboard);
      await tick();
    });

    When('the dashboard mounts', () => {});

    Then('I should see an error alert', async () => {
      await waitFor(() => expect(screen.getByRole('alert')).toBeTruthy(), { timeout: 2000 });
    });

    And('the alert should contain "Falha na rede"', () => {
      expect(screen.getByRole('alert').textContent).toContain('Falha na rede');
    });
  });

  Scenario('No skeleton visible after data loads', ({ Given, When, Then }) => {
    Given('the fetch returns valid stats', async () => {
      global.fetch = vi.fn().mockResolvedValue(
        new Response(JSON.stringify(validStats), { status: 200 }),
      );
      render(Dashboard);
      await tick();
      await waitFor(() => screen.getByText('99.000'));
    });

    When('the dashboard has finished loading', () => {});

    Then('I should not see a loading skeleton', () => {
      expect(document.querySelector('[aria-busy="true"]')).toBeNull();
    });
  });
});
