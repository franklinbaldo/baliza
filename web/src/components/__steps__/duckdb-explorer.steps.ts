import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, fireEvent, waitFor } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { tick } from 'svelte';
import { render } from './shared';
import * as duckdbModule from '../../lib/duckdb';
import * as iaManifestModule from '../../lib/ia-manifest';
import DuckDBExplorerRaw from '../DuckDBExplorer.svelte';
import { FEATURED_QUERIES } from '../../lib/homepage-content';

const DuckDBExplorer = DuckDBExplorerRaw as unknown as Parameters<typeof render>[0];
const feature = await loadFeature('features/duckdb-explorer.feature');

function getTextarea(): HTMLTextAreaElement {
  return screen.getByLabelText('Consulta SQL') as HTMLTextAreaElement;
}

function chipThatNeedsIaUrl() {
  const fq = FEATURED_QUERIES.find((q) => q.sql.includes("'IA_URL'"));
  if (!fq) throw new Error('No featured query with IA_URL placeholder');
  return fq;
}

describeFeature(feature, ({ Scenario, BeforeEachScenario }) => {
  BeforeEachScenario(async () => {
    cleanup();
    vi.restoreAllMocks();
  });

  Scenario('Initial state shows explore button and default query', ({ When, Then, And }) => {
    When('the explorer mounts', async () => {
      render(DuckDBExplorer);
      await tick();
    });

    Then('I should see "Explorar Dados"', () => {
      expect(screen.getByText('Explorar Dados')).toBeTruthy();
    });

    And('the SQL textarea should contain "SELECT"', () => {
      expect(getTextarea().value).toMatch(/SELECT/);
    });
  });

  Scenario("Featured query chip inserts SQL into textarea", ({ When, And, Then }) => {
    When('the explorer mounts', async () => {
      render(DuckDBExplorer);
      await tick();
    });

    let picked: { label: string; sql: string } | null = null;

    And('the user clicks a featured query chip', async () => {
      picked = FEATURED_QUERIES[0];
      await fireEvent.click(screen.getByText(picked.label));
      await tick();
    });

    Then("the SQL textarea should contain the chip's SQL", () => {
      expect(picked).not.toBeNull();
      expect(getTextarea().value).toBe(picked!.sql);
    });
  });

  Scenario('IA_URL placeholder shows warning and disables button', ({ When, And, Then }) => {
    When('the explorer mounts', async () => {
      render(DuckDBExplorer);
      await tick();
    });

    And('the user clicks a featured query chip that still contains "IA_URL"', async () => {
      const fq = chipThatNeedsIaUrl();
      await fireEvent.click(screen.getByText(fq.label));
      await tick();
    });

    Then('I should see a placeholder warning', () => {
      expect(screen.getByRole('note').textContent).toMatch(/IA_URL/);
    });

    And('the explore button should be disabled', () => {
      const btn = screen.getByText('Explorar Dados') as HTMLButtonElement;
      expect(btn.disabled).toBe(true);
    });
  });

  Scenario('Valid SQL executes and renders results', ({ Given, When, And, Then }) => {
    Given('DuckDB returns one row with column "n" equal to 1', () => {
      vi.spyOn(duckdbModule, 'getDuckDB').mockResolvedValue({
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        db: null as any,
        conn: {
          query: vi.fn().mockResolvedValue({
            toArray: () => [{ toJSON: () => ({ n: 1 }) }],
          }),
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
        } as any,
      });
    });

    When('the explorer mounts', async () => {
      render(DuckDBExplorer);
      await tick();
    });

    And('the user clicks "Explorar Dados"', async () => {
      const textarea = getTextarea();
      await fireEvent.input(textarea, { target: { value: 'SELECT 1 AS n' } });
      await tick();
      await fireEvent.click(screen.getByText('Explorar Dados'));
    });

    Then('I should see a results table with "n" as a column header', async () => {
      await waitFor(
        () => {
          const th = document.querySelector('th');
          expect(th?.textContent).toBe('n');
        },
        { timeout: 2000 },
      );
    });
  });

  Scenario(
    'Resolved IA URL replaces IA_URL placeholder in featured query chips',
    ({ Given, When, And, Then }) => {
      const resolvedUrl =
        'https://archive.org/download/baliza-pncp-2025-01/contratos-2025-01.parquet';

      Given(
        'the IA manifest resolves to "https://archive.org/download/baliza-pncp-2025-01/contratos-2025-01.parquet"',
        () => {
          vi.spyOn(iaManifestModule, 'getLatestParquetUrl').mockResolvedValue(
            resolvedUrl,
          );
        },
      );

      When('the explorer mounts', async () => {
        render(DuckDBExplorer);
        await tick();
      });

      And('the user clicks a featured query chip', async () => {
        const fq = FEATURED_QUERIES.find((q) => q.sql.includes("'IA_URL'"));
        if (!fq) throw new Error('No featured query contains IA_URL');
        await waitFor(() => screen.getByText(fq.label));
        await fireEvent.click(screen.getByText(fq.label));
        await tick();
      });

      Then(
        'the SQL textarea should contain the resolved parquet URL',
        async () => {
          await waitFor(() => {
            expect(getTextarea().value).toContain(resolvedUrl);
          });
        },
      );

      And('the SQL textarea should not contain "IA_URL"', () => {
        expect(getTextarea().value).not.toContain('IA_URL');
      });
    },
  );

  Scenario(
    'Single quotes in resolved URL are escaped to prevent SQL injection',
    ({ Given, When, And, Then }) => {
      const maliciousUrl = "https://example.com/evil'; DROP TABLE x; --";

      Given('the IA manifest resolves to a URL containing a single quote', () => {
        vi.spyOn(iaManifestModule, 'getLatestParquetUrl').mockResolvedValue(
          maliciousUrl,
        );
      });

      When('the explorer mounts', async () => {
        render(DuckDBExplorer);
        await tick();
      });

      And('the user clicks a featured query chip', async () => {
        const fq = FEATURED_QUERIES.find((q) => q.sql.includes("'IA_URL'"));
        if (!fq) throw new Error('No featured query contains IA_URL');
        await waitFor(() => screen.getByText(fq.label));
        await fireEvent.click(screen.getByText(fq.label));
        await tick();
      });

      Then(
        'the SQL textarea should contain the URL with doubled single quotes',
        async () => {
          await waitFor(() => {
            expect(getTextarea().value).toContain(
              maliciousUrl.replace(/'/g, "''"),
            );
            expect(getTextarea().value).not.toContain(`'${maliciousUrl}'`);
          });
        },
      );
    },
  );
});
