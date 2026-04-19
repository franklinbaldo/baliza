import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, fireEvent } from '@testing-library/svelte/pure';
import userEvent from '@testing-library/user-event';
import { vi, expect } from 'vitest';
import { render } from './shared';
import ThemeToggleRaw from '../ThemeToggle.svelte';

const ThemeToggle = ThemeToggleRaw as unknown as Parameters<typeof render>[0];

// Mirrors the inline FOUC-prevention script in web/src/layouts/Layout.astro.
// Kept in lockstep: any change in Layout.astro's <script is:inline> block
// must be reflected here so these scenarios exercise real first-visit logic
// instead of a pre-seeded data-theme attribute.
function applyThemeFromEnvironment(): void {
  let stored: string | null = null;
  try { stored = localStorage.getItem('baliza-theme'); } catch { /* unavailable */ }
  if (stored) {
    document.documentElement.setAttribute('data-theme', stored);
    return;
  }
  try {
    if (window.matchMedia('(prefers-color-scheme: light)').matches) {
      document.documentElement.setAttribute('data-theme', 'light');
    }
  } catch { /* unavailable */ }
}

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

    // Astro Layout renders <html data-theme="dark"> as the default; the inline
    // script may overwrite it. Mirror that baseline so the scenarios observe
    // the real first-paint sequence.
    document.documentElement.setAttribute('data-theme', 'dark');
  });

  AfterEachScenario(() => {
    window.matchMedia = originalMatchMedia;
    vi.restoreAllMocks();
  });

  Scenario('Theme toggle persists choice across reloads', ({ Given, When, Then, And }) => {
    Given('the page loads with no stored theme preference', async () => {
      storedTheme = null;
      applyThemeFromEnvironment();
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
    });

    When('the page loads', async () => {
      applyThemeFromEnvironment();
      render(ThemeToggle);
    });

    Then('the document should use the dark theme', () => {
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
      const btn = screen.getByTestId('theme-toggle');
      expect(btn.getAttribute('aria-pressed')).toBe('true');
    });
  });

  Scenario('Theme toggle is operable via keyboard', ({ Given, When, Then, And }) => {
    Given('the theme toggle button is focused', async () => {
      render(ThemeToggle);
      const btn = screen.getByTestId('theme-toggle');
      btn.focus();
    });

    When('the user presses Enter', async () => {
      const user = userEvent.setup();
      await user.keyboard('{Enter}');
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

  Scenario('Theme toggle activates via Space key', ({ Given, When, Then }) => {
    Given('the theme toggle button is focused', async () => {
      render(ThemeToggle);
      const btn = screen.getByTestId('theme-toggle');
      btn.focus();
    });

    When('the user presses Space', async () => {
      const user = userEvent.setup();
      await user.keyboard(' ');
    });

    Then('the theme should toggle', () => {
      const theme = document.documentElement.getAttribute('data-theme');
      expect(theme).toBe('light');
    });
  });

  Scenario('Theme toggle respects prefers-color-scheme light on first visit', ({ Given, And, When, Then }) => {
    Given('the system prefers light color scheme', () => {
      window.matchMedia = vi.fn().mockImplementation((query: string) => ({
        matches: query === '(prefers-color-scheme: light)',
        media: query,
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
      }));
    });

    And('there is no stored theme preference', () => {
      storedTheme = null;
    });

    When('the page loads', async () => {
      applyThemeFromEnvironment();
      render(ThemeToggle);
    });

    Then('the document should use the light theme', () => {
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');
      const btn = screen.getByTestId('theme-toggle');
      expect(btn.getAttribute('aria-pressed')).toBe('false');
    });
  });

  Scenario('Stored theme preference is restored on page load', ({ Given, When, Then }) => {
    Given('a stored theme preference of "light" exists', () => {
      storedTheme = 'light';
    });

    When('the toggle mounts', async () => {
      applyThemeFromEnvironment();
      render(ThemeToggle);
    });

    Then('the button reflects the stored light theme', () => {
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');
      const btn = screen.getByTestId('theme-toggle');
      expect(btn.getAttribute('aria-pressed')).toBe('false');
    });
  });
});
