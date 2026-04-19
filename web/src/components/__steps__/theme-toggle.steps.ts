import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, fireEvent } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { render } from './shared';
import ThemeToggleRaw from '../ThemeToggle.svelte';

const ThemeToggle = ThemeToggleRaw as unknown as Parameters<typeof render>[0];

const feature = await loadFeature('features/theme-toggle.feature');

describeFeature(feature, ({ Scenario, BeforeEachScenario, AfterEachScenario }) => {
  let originalMatchMedia: typeof window.matchMedia;
  let storedTheme: string | null = null;

  BeforeEachScenario(async () => {
    cleanup();
    storedTheme = null;
    originalMatchMedia = window.matchMedia;

    vi.spyOn(Storage.prototype, 'getItem').mockImplementation((key) => {
      if (key === 'baliza-theme') return storedTheme;
      return null;
    });
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation((key, value) => {
      if (key === 'baliza-theme') storedTheme = value;
    });

    document.documentElement.removeAttribute('data-theme');
  });

  AfterEachScenario(() => {
    window.matchMedia = originalMatchMedia;
    vi.restoreAllMocks();
  });

  Scenario('Theme toggle persists choice across reloads', ({ Given, When, Then, And }) => {
    Given('the page loads with no stored theme preference', async () => {
      storedTheme = null;
      document.documentElement.setAttribute('data-theme', 'dark');
      render(ThemeToggle);
    });

    When('the user clicks the theme toggle button', async () => {
      const btn = screen.getByTestId('theme-toggle');
      await fireEvent.click(btn);
    });

    Then('the theme should change', () => {
      const theme = document.documentElement.getAttribute('data-theme');
      expect(theme).toBe('light');
    });

    And('the new theme should persist after page reload', () => {
      expect(storedTheme).toBe('light');
    });
  });

  Scenario('Theme toggle respects prefers-color-scheme on first visit', ({ Given, And, When, Then }) => {
    Given('the system prefers dark color scheme', () => {
      window.matchMedia = vi.fn().mockImplementation((query: string) => ({
        matches: query === '(prefers-color-scheme: dark)',
        media: query,
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
      }));
    });

    And('there is no stored theme preference', () => {
      storedTheme = null;
      document.documentElement.setAttribute('data-theme', 'dark');
    });

    When('the page loads', async () => {
      render(ThemeToggle);
    });

    Then('the document should use the dark theme', () => {
      const btn = screen.getByTestId('theme-toggle');
      expect(btn.getAttribute('aria-pressed')).toBe('true');
    });
  });

  Scenario('Theme toggle is operable via keyboard', ({ Given, When, Then, And }) => {
    Given('the theme toggle button is focused', async () => {
      document.documentElement.setAttribute('data-theme', 'dark');
      render(ThemeToggle);
      const btn = screen.getByTestId('theme-toggle');
      btn.focus();
    });

    When('the user presses Enter', async () => {
      const btn = screen.getByTestId('theme-toggle');
      await fireEvent.keyDown(btn, { key: 'Enter', code: 'Enter' });
      await fireEvent.click(btn);
    });

    Then('the theme should toggle', () => {
      const theme = document.documentElement.getAttribute('data-theme');
      expect(theme).toBe('light');
    });

    And('the button should reflect the current theme state via aria-pressed', () => {
      const btn = screen.getByTestId('theme-toggle');
      expect(btn.getAttribute('aria-pressed')).toBe('false');
    });
  });
});
