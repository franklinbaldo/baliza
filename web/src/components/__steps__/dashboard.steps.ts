import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, waitFor } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { tick } from 'svelte';
import { render } from './shared';
import DashboardRaw from '../Dashboard.svelte';

const Dashboard = DashboardRaw as unknown as Parameters<typeof render>[0];
const feature = await loadFeature('features/dashboard.feature');

const validStats = { total_contracts: 42000, total_quarantine: 3, days_on_ia: 730 };

describeFeature(feature, ({ Scenario, BeforeEachScenario }) => {
  BeforeEachScenario(async () => {
    cleanup();
    vi.restoreAllMocks();
  });

  Scenario('Show loading skeletons while fetching', ({ Given, When, Then }) => {
    Given('the fetch is pending', async () => {
      global.fetch = vi.fn().mockReturnValue(new Promise(() => {}));
      render(Dashboard);
      await tick();
    });

    When('the dashboard mounts', () => {});

    Then('I should see 3 skeleton cards', async () => {
      await waitFor(() => {
        expect(document.querySelector('[aria-busy="true"]')).toBeTruthy();
      });
    });
  });

  Scenario('Render stats when fetch succeeds', ({ Given, When, Then }) => {
    Given('the fetch returns total_contracts 42000', async () => {
      global.fetch = vi.fn().mockResolvedValue(
        new Response(JSON.stringify(validStats), { status: 200 }),
      );
      render(Dashboard);
      await tick();
    });

    When('the dashboard mounts', () => {});

    Then('I should see "42000" in a stat card', async () => {
      await waitFor(() => expect(screen.getByText('42000')).toBeTruthy());
    });
  });

  Scenario('Show AlertBanner on fetch error', ({ Given, When, Then, And }) => {
    Given('the fetch throws "Falha na rede"', async () => {
      global.fetch = vi.fn().mockRejectedValue(new Error('Falha na rede'));
      render(Dashboard);
      await tick();
    });

    When('the dashboard mounts', () => {});

    Then('I should see an alert banner with level error', async () => {
      await waitFor(() => expect(screen.getByRole('alert')).toBeTruthy(), { timeout: 2000 });
    });

    And('the alert should contain "Falha na rede"', () => {
      expect(screen.getByRole('alert').textContent).toContain('Falha na rede');
    });
  });

  Scenario('Skeleton cards not visible after data loads', ({ Given, When, Then }) => {
    Given('the fetch returns valid stats', async () => {
      global.fetch = vi.fn().mockResolvedValue(
        new Response(JSON.stringify(validStats), { status: 200 }),
      );
      render(Dashboard);
      await tick();
      await waitFor(() => screen.getByText('42000'));
    });

    When('the dashboard has finished loading', () => {});

    Then('I should not see any aria-busy element', () => {
      expect(document.querySelector('[aria-busy="true"]')).toBeNull();
    });
  });
});
